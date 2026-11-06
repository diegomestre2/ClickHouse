#include <Processors/QueryPlan/Fragments/FragmentGraphExplain.h>

#include <Processors/QueryPlan/Fragments/FragmentGraph.h>
#include <Processors/QueryPlan/Fragments/PreparedFragmentGraph.h>
#include <Processors/QueryPlan/Fragments/TextCanvas.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <IO/Operators.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <limits>
#include <map>
#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// The text of a step box (its name and description) is wrapped to lines of this many columns.
constexpr size_t max_step_text_width = 40;
/// The detail lines of a step box are cut to this many columns.
constexpr size_t max_detail_width = 60;
/// The first line of an exchange label (name and kind) is cut to this many columns.
constexpr size_t max_label_width = 48;
/// Columns between a step box and the frame of its fragment.
constexpr Int64 frame_padding = 2;
/// A step whose subtree is wider than this is stacked when that makes it narrower (see `TreeBuilder::tryStack`).
constexpr Int64 max_tree_width = 120;
/// Columns between a stacked box and the subtrees of its inputs on the right.
constexpr Int64 side_gap = 2;

struct StepBox
{
    /// The whole text of the step: its name and description.
    String text;
    /// `text` wrapped to `max_step_text_width` columns, one line per inner line of the box.
    std::vector<String> lines;
    /// The lines `EXPLAIN` prints under the step, drawn under the text.
    std::vector<String> details;
    /// Index in `FragmentGraph::fragments`.
    size_t fragment = 0;
    /// Inputs in the plan, then, for a receive step, the send step of the producer fragment.
    std::vector<size_t> children;
    /// Set for the root box of a fragment that is drawn under a receive step: index in `FragmentGraph::exchanges`.
    std::optional<size_t> incoming_exchange;
    /// Set for a stacked box: the index in `children` of the input drawn straight under the box. The other inputs are
    /// drawn to the right of the box, under its bottom, and the input under the box starts under them.
    std::optional<size_t> spine;
    /// For a stacked box: the column of the edge from the input on the right, which enters the right border of the box.
    Int64 side_edge_x = 0;
    /// A join step: the edges of a stacked join say which input is `left` and which is `right`.
    bool is_join = false;
    /// For a box with inputs in other fragments: the first line under the frame of its fragment where the edges to
    /// those inputs branch or their labels start.
    size_t producers_y = 0;
    size_t width = 0;
    Int64 center = 0;
    size_t y = 0;

    Int64 left() const { return center - static_cast<Int64>(width / 2); }
    Int64 right() const { return left() + static_cast<Int64>(width) - 1; }
    size_t height() const { return TextCanvas::boxHeight(lines.size(), details.size()); }
    /// The line of the bottom border.
    size_t bottom() const { return y + height() - 1; }

    /// Sets `text`, wraps it into `lines` and updates `width`.
    void setText(String new_text)
    {
        text = std::move(new_text);
        lines = TextCanvas::wrapToWidth(text, max_step_text_width);
        updateWidth();
    }

    void updateWidth()
    {
        width = 0;
        for (const auto & line : lines)
            width = std::max(width, TextCanvas::displayWidth(line));
        for (const auto & detail : details)
            width = std::max(width, TextCanvas::displayWidth(detail));
        width += 4;
    }
};

/// A nested plan of a step (`IQueryPlanStep::getChildPlans`), drawn after the main drawing as a plan of its own. Its id
/// is `N<index + 1>`.
struct NestedPlan
{
    const QueryPlan * plan = nullptr;
    /// The name and description of the step that owns the plan.
    String owner;
};

using NestedPlans = std::vector<NestedPlan>;

struct FragmentFrame
{
    String title;
    size_t root_box = 0;
    size_t bottom = 0;
};

class TreeBuilder
{
public:
    /// The nested plans of the drawn steps are appended to `nested_plans_`, which is shared by the builders of the main
    /// drawing and of the nested plans, so that the ids are unique in the whole output.
    TreeBuilder(
        const PreparedFragmentGraph & prepared_, size_t max_step_description_length_, NestedPlans & nested_plans_, bool frameless_)
        : prepared(prepared_)
        , graph(prepared.graph)
        , options(prepared.options)
        , nested_plans(nested_plans_)
        , frameless(frameless_)
        , padding(frameless ? 0 : frame_padding)
        , sibling_gap(3 + 2 * padding)
        , max_step_description_length(max_step_description_length_)
    {
        for (size_t i = 0; i < graph.fragments.size(); ++i)
            fragment_index_by_id[graph.fragments[i].id] = i;
    }

    /// Draws the fragments of the graph and the exchanges between them.
    void drawGraph(WriteBuffer & buf)
    {
        buildBoxes();
        attachProducers();
        drawFrom(rootFragment(), buf);
    }

    /// Draws a nested plan as bare boxes. Must be built with `frameless`.
    void drawNestedPlan(const QueryPlan & plan, WriteBuffer & buf)
    {
        chassert(frameless);
        if (!plan.getRootNode())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A nested plan to draw has no root node");
        frames.resize(1);
        frames[0].root_box = addPlanBoxes(*plan.getRootNode(), 0, prepared.getPrettyNames(plan));
        drawFrom(0, buf);
    }

private:
    void drawFrom(size_t root_fragment, WriteBuffer & buf)
    {
        const size_t root = frames[root_fragment].root_box;
        layoutColumns(root, 0);
        Int64 min_left = 0;
        for (size_t i = 0; i < frames.size(); ++i)
            min_left = std::min(min_left, frameLeft(i));
        if (min_left < 0)
            shiftSubtree(root, -min_left);

        /// The title of the root frame is on line 0, its first box on line 2. Without a frame the first box is on line 0.
        layoutRows(root_fragment, frameless ? 0 : 2);

        TextCanvas canvas;
        if (!frameless)
            for (size_t i = 0; i < frames.size(); ++i)
                drawFrame(canvas, i);
        for (const auto & step_box : boxes)
            canvas.box(static_cast<size_t>(step_box.left()), step_box.y, step_box.width, step_box.lines, step_box.details);
        for (size_t i = 0; i < boxes.size(); ++i)
            drawEdges(canvas, i);
        canvas.writeTo(buf);
    }

    const PreparedFragmentGraph & prepared;
    const FragmentGraph & graph;
    const ExplainPlanOptions & options;
    NestedPlans & nested_plans;
    /// A plan that was not cut is one fragment, drawn as bare boxes: no frame, no title, no padding for the frame.
    const bool frameless;
    /// Columns between a step box and the frame of its fragment: `frame_padding`, or 0 without a frame.
    const Int64 padding;
    /// Columns between the subtrees of two inputs of a step; the subtrees already include the padding.
    const Int64 sibling_gap;
    /// A description is cut to this many bytes, as in `QueryPlan::explainPlan`; 0 means no limit.
    const size_t max_step_description_length;
    std::unordered_map<UInt32, size_t> fragment_index_by_id;

    std::vector<StepBox> boxes;
    std::vector<FragmentFrame> frames;
    /// (fragment index, exchange name) -> the receive step box of that exchange.
    std::map<std::pair<size_t, String>, size_t> receive_boxes;

    size_t fragmentIndex(UInt32 id) const
    {
        auto it = fragment_index_by_id.find(id);
        if (it == fragment_index_by_id.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Fragment {} is not found in the fragment graph", id);
        return it->second;
    }

    /// The fragment that no exchange reads. The drawing hangs every other fragment under it, so it must be the only one:
    /// a second root would never be laid out.
    size_t rootFragment() const
    {
        std::vector<bool> is_producer(graph.fragments.size(), false);
        for (const auto & exchange : graph.exchanges)
            is_producer[fragmentIndex(exchange.from)] = true;
        std::vector<UInt32> roots;
        for (size_t i = 0; i < graph.fragments.size(); ++i)
            if (!is_producer[i])
                roots.push_back(graph.fragments[i].id);
        if (roots.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Cannot draw the fragment graph: it must have exactly one fragment that no exchange reads, found [{}]",
                fmt::join(roots, ", "));
        return fragmentIndex(roots.front());
    }

    /// The name of the step and its description. The nested plans of the step are registered in `nested_plans`, to be
    /// drawn after the main drawing, and the text ends with their ids: `← N1, N2`. Must be called once per step.
    String stepText(IQueryPlanStep & step)
    {
        String text = step.getName();
        String description(step.getStepDescription());
        if (options.pretty)
            description = QueryPlanFormat::trimColumnIdentifier(description);
        if (max_step_description_length)
            description = description.substr(0, max_step_description_length);
        if (options.description && !description.empty())
            text += fmt::format(" ({})", description);

        std::vector<String> ids;
        for (const auto * child_plan : step.getChildPlans(/*for_explain=*/ true))
        {
            /// `QueryPlanFormat::buildPrettyNamesPerPlan` skips such plans too.
            if (!child_plan || !child_plan->getRootNode())
                continue;
            nested_plans.push_back({child_plan, text});
            ids.push_back(fmt::format("N{}", nested_plans.size()));
        }
        if (!ids.empty())
            text += fmt::format(" ← {}", fmt::join(ids, ", "));
        return text;
    }

    /// The lines plain `EXPLAIN` prints under the step with the same options, each cut to `max_detail_width`.
    std::vector<String> stepDetails(IQueryPlanStep & step, const PrettyNames & names) const
    {
        WriteBufferFromOwnString out;
        IQueryPlanStep::FormatSettings settings{
            .out = out,
            .header_prefix = "",
            .detail_prefix = "",
            .write_header = options.header,
            .compact = options.compact,
            .pretty = options.pretty,
            .pretty_names = names.pretty_names,
            .runtime_filter_names = names.runtime_filter_names};
        explainStepDetails(step, settings, options);

        std::vector<String> details;
        std::string_view text = out.str();
        while (!text.empty())
        {
            size_t end = text.find('\n');
            std::string_view line = text.substr(0, end);
            if (!line.empty())
                details.push_back(TextCanvas::cutToWidth(line, max_detail_width));
            if (end == std::string_view::npos)
                break;
            text.remove_prefix(end + 1);
        }
        return details;
    }

    size_t addBox(String text, std::vector<String> details, size_t fragment)
    {
        StepBox step_box;
        step_box.details = std::move(details);
        step_box.setText(std::move(text));
        step_box.fragment = fragment;
        boxes.push_back(std::move(step_box));
        return boxes.size() - 1;
    }

    /// An `Expression` step is never drawn: the box of its input takes its place, as in plain `EXPLAIN` with `compact`.
    static const QueryPlan::Node & skipExpressions(const QueryPlan::Node & node)
    {
        const QueryPlan::Node * current = &node;
        while (current->step->getName() == "Expression" && current->children.size() == 1)
            current = current->children.front();
        return *current;
    }

    size_t addPlanBoxes(const QueryPlan::Node & plan_node, size_t fragment, const PrettyNames & names)
    {
        const auto & node = skipExpressions(plan_node);
        size_t index = addBox(stepText(*node.step), stepDetails(*node.step, names), fragment);
        boxes[index].is_join = typeid_cast<const JoinStep *>(node.step.get()) || typeid_cast<const JoinStepLogical *>(node.step.get());
        if (const auto * exchange_id = PreparedFragmentGraph::tryGetReceiveExchangeId(*node.step))
            receive_boxes[{fragment, *exchange_id}] = index;
        for (const auto * child : node.children)
        {
            size_t child_index = addPlanBoxes(*child, fragment, names);
            boxes[index].children.push_back(child_index);
        }
        return index;
    }

    void buildBoxes()
    {
        frames.resize(graph.fragments.size());
        for (size_t i = 0; i < graph.fragments.size(); ++i)
        {
            const auto & fragment = graph.fragments[i];
            auto & frame = frames[i];
            frame.title = fmt::format("F{} · ", fragment.id);
            frame.title += fragment.description ? "set: " + *fragment.description : fragment.stage_name;
            if (fragment.execution_mode == QueryFragment::ExecutionMode::SingleNode)
                frame.title += " · single node";
            if (fragment.fallback_reason)
                frame.title += " · not distributed: " + *fragment.fallback_reason;
            frame.title = TextCanvas::replaceControlCharacters(frame.title);

            if (!fragment.plan || !fragment.plan->getRootNode())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Fragment {} has no plan to draw", fragment.id);
            frames[i].root_box = addPlanBoxes(*fragment.plan->getRootNode(), i, prepared.getPrettyNames(*fragment.plan));
        }
    }

    /// The distance of a fragment from the root fragment along its longest chain of consumers.
    size_t fragmentDepth(size_t fragment, std::vector<std::optional<size_t>> & depths) const
    {
        if (depths[fragment])
            return *depths[fragment];
        size_t depth = 0;
        for (const auto & exchange : graph.exchanges)
            if (exchange.from == graph.fragments[fragment].id)
                depth = std::max(depth, fragmentDepth(fragmentIndex(exchange.to), depths) + 1);
        depths[fragment] = depth;
        return depth;
    }

    /// The box of `consumer` that an exchange ends at: the receive step of the exchange.
    size_t findReceiveBox(size_t consumer, const FragmentExchange & exchange) const
    {
        auto it = receive_boxes.find({consumer, exchange.name});
        if (it == receive_boxes.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Cannot draw exchange {}: fragment {} has no receive step for it", exchange.name, exchange.to);
        return it->second;
    }

    /// Hangs every fragment under one consumer: the deepest one, so that the fragment is drawn below all its readers.
    /// The other consumers get a note in their receive step and in the producer's title instead of a second edge.
    void attachProducers()
    {
        std::vector<std::optional<size_t>> depths(graph.fragments.size());
        std::vector<std::optional<size_t>> primary_exchange(graph.fragments.size());
        for (size_t i = 0; i < graph.exchanges.size(); ++i)
        {
            const auto & exchange = graph.exchanges[i];
            size_t producer = fragmentIndex(exchange.from);
            size_t consumer = fragmentIndex(exchange.to);
            auto & primary = primary_exchange[producer];
            if (!primary)
            {
                primary = i;
                continue;
            }
            size_t primary_consumer = fragmentIndex(graph.exchanges[*primary].to);
            size_t depth = fragmentDepth(consumer, depths);
            size_t primary_depth = fragmentDepth(primary_consumer, depths);
            if (depth > primary_depth || (depth == primary_depth && exchange.to < graph.exchanges[*primary].to))
                primary = i;
        }

        for (size_t i = 0; i < graph.exchanges.size(); ++i)
        {
            const auto & exchange = graph.exchanges[i];
            size_t producer = fragmentIndex(exchange.from);
            size_t consumer = fragmentIndex(exchange.to);
            const size_t receive_box = findReceiveBox(consumer, exchange);

            if (primary_exchange[producer] == i)
            {
                boxes[receive_box].children.push_back(frames[producer].root_box);
                boxes[frames[producer].root_box].incoming_exchange = i;
            }
            else
            {
                auto & step_box = boxes[receive_box];
                step_box.setText(fmt::format("{} ← F{}", step_box.text, exchange.from));
                frames[producer].title += fmt::format(" · also read by F{}", exchange.to);
            }
        }
    }

    bool crossesFragments(size_t parent, size_t child) const { return boxes[parent].fragment != boxes[child].fragment; }

    std::vector<String> labelLines(size_t exchange_index) const
    {
        const auto & exchange = graph.exchanges[exchange_index];
        std::vector<String> lines;
        lines.push_back(TextCanvas::cutToWidth(
            fmt::format("{} · {}", exchange.name, prepared.formatExchangeKind(exchange)), max_label_width));
        String mode(PreparedFragmentGraph::formatExchangeMode(exchange.mode));
        /// A persisted exchange is written to storage before its consumer starts: `⛁` marks it next to the mode, which
        /// still says it in words, so a terminal without the glyph loses nothing but the mark.
        if (exchange.mode == ExchangeDescription::Kind::Persisted)
            mode = "⛁ " + mode;
        if (exchange.source_buckets || exchange.destination_buckets)
            mode += fmt::format(" · {} → {}", exchange.source_buckets, exchange.destination_buckets);
        lines.push_back(std::move(mode));
        if (options.estimates && exchange.statistics.estimated_rows)
            lines.push_back(fmt::format("~{} rows", *exchange.statistics.estimated_rows));
        return lines;
    }

    size_t labelWidth(size_t exchange_index) const
    {
        size_t width = 0;
        for (const auto & line : labelLines(exchange_index))
            width = std::max(width, TextCanvas::displayWidth(line));
        return width;
    }

    void shiftSubtree(size_t index, Int64 offset)
    {
        boxes[index].center += offset;
        /// Used only by a stacked box, but kept in step with the centre for every box.
        boxes[index].side_edge_x += offset;
        for (size_t child : boxes[index].children)
            shiftSubtree(child, offset);
    }

    Int64 frameLeft(size_t fragment) const
    {
        Int64 left = std::numeric_limits<Int64>::max();
        for (const auto & step_box : boxes)
            if (step_box.fragment == fragment)
                left = std::min(left, step_box.left());
        left -= padding;

        /// The edge into the fragment crosses the top border at the centre of the root box: `┌─ title ` must end before.
        const auto & root_box = boxes[frames[fragment].root_box];
        if (root_box.incoming_exchange)
            left = std::min(left, root_box.center - static_cast<Int64>(TextCanvas::displayWidth(frames[fragment].title)) - 4);
        return left;
    }

    Int64 frameRight(size_t fragment) const
    {
        Int64 right = std::numeric_limits<Int64>::min();
        for (const auto & step_box : boxes)
            if (step_box.fragment == fragment)
                right = std::max(right, step_box.right());
        if (frameless)
            return right;
        /// `┌─ title ┐` needs the title and 5 more columns.
        return std::max(right + frame_padding, frameLeft(fragment) + static_cast<Int64>(TextCanvas::displayWidth(frames[fragment].title)) + 4);
    }

    /// A box with two inputs, both in its own fragment, can be stacked: a chain of such boxes is what stacking makes
    /// narrower. A fan-out (a `Union` of many inputs) is not stacked: its inputs would still be side by side. The edges
    /// to the inputs in other fragments leave the frame straight down, so a receive step is not stacked.
    bool canStack(size_t index) const
    {
        const auto & children = boxes[index].children;
        if (children.size() != 2)
            return false;
        return std::none_of(children.begin(), children.end(), [&](size_t child) { return crossesFragments(index, child); });
    }

    /// Whether a box of the subtree of `index` in its fragment has an input in another fragment.
    bool hasProducers(size_t index) const
    {
        for (size_t child : boxes[index].children)
            if (crossesFragments(index, child) || hasProducers(child))
                return true;
        return false;
    }

    /// The rightmost column of the boxes of the subtree of `index` in its fragment.
    Int64 fragmentSubtreeRight(size_t index) const
    {
        Int64 right = boxes[index].right();
        for (size_t child : boxes[index].children)
            if (!crossesFragments(index, child))
                right = std::max(right, fragmentSubtreeRight(child));
        return right;
    }

    /// The label of the edge of a stacked join from its input `child_position` (0 for the left input).
    static std::string_view joinSideLabel(size_t child_position) { return child_position == 0 ? "left" : "right"; }

    /// A tidy tree puts every leaf in its own columns, so a chain of steps with two inputs, such as the chain of joins
    /// of a reordered join, grows to the right by the width of every side input. Stacking a box draws its wider input
    /// (the spine) straight under it and its other input to the right of the box, on the lines under the box and above
    /// the spine. The subtrees on the right of the boxes of a chain are then on different lines, in the same columns,
    /// so the width is their maximum instead of their sum.
    ///
    ///     ┌──────┐
    ///     │ Join ◀─ right ─┐
    ///     └──▲───┘         │
    ///        │ left    ┌───┴───┐
    ///        │         │ Read  │
    ///        │         └───────┘
    ///     ┌──┴───┐
    ///     │ Join │ ...
    ///
    /// The two inputs are laid out tidy in `[child_lefts[i], child_rights[i])`. Rearranges them if the stacked subtree
    /// is narrower than the tidy one, and returns the first free column to the right of the subtree.
    ///
    /// If the subtree on the right reads other fragments, it starts right of every box of the spine in the fragment:
    /// its edges go down to the bottom of the frame past the spine, and the fragments it reads are drawn before the
    /// fragments read by the spine (see `layoutSubtreeRows`), so the edges of the spine pass them on the left.
    Int64 tryStack(
        size_t index, Int64 left, Int64 tidy_right, const std::vector<Int64> & child_lefts, const std::vector<Int64> & child_rights)
    {
        chassert(boxes[index].children.size() == 2);
        const size_t spine = child_rights[1] - child_lefts[1] > child_rights[0] - child_lefts[0] ? 1 : 0;
        const size_t side = 1 - spine;
        const size_t spine_child = boxes[index].children[spine];
        const size_t side_child = boxes[index].children[side];
        const Int64 tidy_center = boxes[index].center;

        /// The spine at the left bound of the subtree, the box over it.
        Int64 spine_shift = left - child_lefts[spine];
        shiftSubtree(spine_child, spine_shift);
        boxes[index].center = boxes[spine_child].center;
        /// A box wider than its spine must not cross the left bound of its subtree.
        Int64 overflow = left + padding - boxes[index].left();
        if (overflow > 0)
        {
            shiftSubtree(spine_child, overflow);
            spine_shift += overflow;
            boxes[index].center += overflow;
        }
        const auto & stacked = boxes[index];

        /// The other input right of the box. The edge leaves the right border of the box on its first line,
        /// `◀─ right ─┐` (`◀──┐` for a step that is not a join), and goes down to the centre of the input.
        Int64 side_left = stacked.right() + 1 + side_gap;
        if (hasProducers(side_child))
            side_left = std::max(side_left, fragmentSubtreeRight(spine_child) + 1 + side_gap);
        Int64 side_shift = side_left - child_lefts[side];
        Int64 label_width = stacked.is_join ? static_cast<Int64>(joinSideLabel(side).size()) + 2 : 0;
        Int64 min_edge_x = stacked.right() + 3 + label_width;
        Int64 edge_x = boxes[side_child].center + side_shift;
        if (edge_x < min_edge_x)
        {
            side_shift += min_edge_x - edge_x;
            edge_x = min_edge_x;
        }
        shiftSubtree(side_child, side_shift);

        Int64 stacked_right
            = std::max({child_rights[spine] + spine_shift, child_rights[side] + side_shift, stacked.right() + 1 + padding});
        if (stacked_right < tidy_right)
        {
            boxes[index].spine = spine;
            boxes[index].side_edge_x = edge_x;
            return stacked_right;
        }

        shiftSubtree(spine_child, -spine_shift);
        shiftSubtree(side_child, -side_shift);
        boxes[index].center = tidy_center;
        return tidy_right;
    }

    /// Places the subtree of `index` in the columns from `left` on, as a tidy tree: leaves left to right, a step centred
    /// between its first and last input. Returns the first free column to the right of the subtree.
    Int64 layoutColumns(size_t index, Int64 left)
    {
        auto & step_box = boxes[index];
        const auto width = static_cast<Int64>(step_box.width);
        /// The right bound of a leaf; a step with inputs computes it from the subtrees of its inputs below.
        Int64 right = left + width + 2 * padding;
        if (step_box.children.empty())
        {
            step_box.center = left + padding + width / 2;
        }
        else
        {
            /// The bounds of the subtree of every input, to rearrange them if the box is stacked.
            std::vector<Int64> child_lefts;
            std::vector<Int64> child_rights;
            Int64 x = left;
            for (size_t child : step_box.children)
            {
                child_lefts.push_back(x);
                x = layoutColumns(child, x);
                child_rights.push_back(x);
                x += sibling_gap;
            }
            right = x - sibling_gap;

            auto & placed = boxes[index];
            placed.center = (boxes[placed.children.front()].center + boxes[placed.children.back()].center) / 2;
            /// A box wider than its inputs must not cross the left bound of its subtree.
            Int64 overflow = left + padding - placed.left();
            if (overflow > 0)
            {
                shiftSubtree(index, overflow);
                right += overflow;
                for (size_t i = 0; i < child_lefts.size(); ++i)
                {
                    child_lefts[i] += overflow;
                    child_rights[i] += overflow;
                }
            }
            right = std::max(right, placed.right() + 1 + padding);

            if (right - left > max_tree_width && canStack(index))
                right = tryStack(index, left, right, child_lefts, child_rights);
        }

        /// The frame of a fragment that an edge enters reaches left of its boxes when its title is long (see `frameLeft`), and
        /// must not cross the left bound of the subtree either.
        if (frames[boxes[index].fragment].root_box == index)
        {
            Int64 overflow = left - frameLeft(boxes[index].fragment);
            if (overflow > 0)
            {
                shiftSubtree(index, overflow);
                right += overflow;
            }
        }

        const auto & placed = boxes[index];
        /// The label of the edge into this fragment is written to the right of the edge, inside this subtree.
        if (placed.incoming_exchange)
            right = std::max(right, placed.center + 2 + static_cast<Int64>(labelWidth(*placed.incoming_exchange)) + 1);
        /// The title of the frame must fit as well.
        if (frames[placed.fragment].root_box == index)
            right = std::max(right, frameRight(placed.fragment) + 1);
        return right;
    }

    /// An edge from a box to the root box of a fragment it reads, drawn under the frame of the box.
    struct ProducerEdge
    {
        size_t receive_box;
        size_t producer_root;
    };

    /// The fragments read by a fragment, by band: the fragments of a band are drawn under the fragments of the
    /// previous band, so that the edges of a stacked box to the fragments read by its spine pass the fragments read by
    /// its inputs on the right (see `tryStack`). Without a stacked box, every fragment is in band 0.
    using ProducerBands = std::vector<std::vector<ProducerEdge>>;

    /// Assigns lines to the boxes of the subtree of `index` in its fragment, with the box on line `y`. The fragments
    /// that the subtree reads are added to `bands`, from band `band` on. Returns the bottom line of the subtree and
    /// sets `last_band` to the last band it used.
    size_t layoutSubtreeRows(size_t index, size_t y, size_t band, ProducerBands & bands, size_t & last_band)
    {
        boxes[index].y = y;
        size_t bottom = boxes[index].bottom();
        last_band = band;

        std::vector<size_t> in_fragment;
        for (size_t child : boxes[index].children)
        {
            if (crossesFragments(index, child))
            {
                if (bands.size() <= band)
                    bands.resize(band + 1);
                bands[band].push_back({index, child});
            }
            else
                in_fragment.push_back(child);
        }

        if (!boxes[index].spine)
        {
            /// One input: `▲`, `│`, then the input box. Several: one more line for the branch.
            size_t child_y = boxes[index].bottom() + 1 + (in_fragment.size() > 1 ? 2 : 1);
            for (size_t child : in_fragment)
            {
                size_t child_last_band = band;
                bottom = std::max(bottom, layoutSubtreeRows(child, child_y, band, bands, child_last_band));
                last_band = std::max(last_band, child_last_band);
            }
            return bottom;
        }

        /// A stacked box: the input on the right starts two lines under the box, and the spine starts under it. The
        /// fragments read by the spine are drawn after the fragments read by the input on the right.
        const size_t spine_child = boxes[index].children[*boxes[index].spine];
        const size_t side_child = boxes[index].children[1 - *boxes[index].spine];
        size_t side_last_band = band;
        const size_t side_bottom = layoutSubtreeRows(side_child, boxes[index].bottom() + 2, band, bands, side_last_band);
        size_t spine_last_band = side_last_band + 1;
        bottom = std::max(side_bottom, layoutSubtreeRows(spine_child, side_bottom + 2, side_last_band + 1, bands, spine_last_band));
        last_band = spine_last_band;
        return bottom;
    }

    /// Assigns lines to the boxes of `fragment`, whose root box is on line `top_box_y`, and then to the fragments
    /// drawn under it. Returns the bottom line of the drawing of the fragment and of the fragments under it.
    size_t layoutRows(size_t fragment, size_t top_box_y)
    {
        ProducerBands bands;
        size_t last_band = 0;
        const size_t bottom = layoutSubtreeRows(frames[fragment].root_box, top_box_y, 0, bands, last_band);

        auto & frame = frames[fragment];
        frame.bottom = bottom + 1;

        size_t previous_bottom = frame.bottom;
        for (const auto & band : bands)
        {
            if (band.empty())
                continue;

            size_t label_height = 0;
            std::map<size_t, size_t> producers_per_box;
            for (const auto & edge : band)
            {
                label_height = std::max(label_height, labelLines(*boxes[edge.producer_root].incoming_exchange).size());
                ++producers_per_box[edge.receive_box];
            }
            bool branches_to_producers = std::any_of(
                producers_per_box.begin(), producers_per_box.end(), [](const auto & entry) { return entry.second > 1; });

            /// Under the frame, or under the fragments of the previous band: the branch line if one box has several
            /// producers, the labels, the producer frame top with its title, an empty line, then the first box of the
            /// producer.
            const size_t producers_y = previous_bottom + 1;
            const size_t producer_top = producers_y + (branches_to_producers ? 1 : 0) + label_height;
            size_t band_bottom = previous_bottom;
            for (const auto & edge : band)
            {
                boxes[edge.receive_box].producers_y = producers_y;
                band_bottom = std::max(band_bottom, layoutRows(boxes[edge.producer_root].fragment, producer_top + 2));
            }
            previous_bottom = band_bottom;
        }
        return previous_bottom;
    }

    void drawFrame(TextCanvas & canvas, size_t fragment)
    {
        const auto & frame = frames[fragment];
        Int64 left = frameLeft(fragment);
        Int64 right = frameRight(fragment);
        size_t top = boxes[frame.root_box].y - 2;
        canvas.frame(static_cast<size_t>(left), top, static_cast<size_t>(right - left + 1), frame.bottom - top + 1, frame.title);
    }

    /// The branch line under a step with several inputs: `┌───┴───┐`, with `┬` above the middle inputs.
    static void drawBranch(TextCanvas & canvas, size_t y, Int64 parent_center, const std::vector<Int64> & child_centers)
    {
        for (Int64 x = child_centers.front(); x <= child_centers.back(); ++x)
            canvas.put(static_cast<size_t>(x), y, "─");
        for (size_t i = 0; i < child_centers.size(); ++i)
        {
            const char * glyph = i == 0 ? "┌" : (i + 1 == child_centers.size() ? "┐" : "┬");
            canvas.put(static_cast<size_t>(child_centers[i]), y, glyph);
        }
        bool parent_over_child = std::find(child_centers.begin(), child_centers.end(), parent_center) != child_centers.end();
        canvas.put(static_cast<size_t>(parent_center), y, parent_over_child ? "┼" : "┴");
    }

    /// The edges of a stacked box (see `tryStack`) to its two inputs, which are in its fragment: straight down to the
    /// spine, and from the right border to the input on the right. A join labels both edges with the side of the input.
    void drawStackedEdges(TextCanvas & canvas, size_t index)
    {
        const auto & step_box = boxes[index];
        const size_t spine_position = *step_box.spine;
        const auto & spine = boxes[step_box.children[spine_position]];
        const auto center = static_cast<size_t>(step_box.center);

        for (size_t y = step_box.bottom() + 1; y < spine.y; ++y)
            canvas.put(center, y, "│");
        canvas.put(static_cast<size_t>(spine.center), spine.y, "┴");
        if (step_box.is_join)
            canvas.put(center + 2, step_box.bottom() + 1, joinSideLabel(spine_position));

        const auto & side = boxes[step_box.children[1 - spine_position]];
        const auto right = static_cast<size_t>(step_box.right());
        const auto edge_x = static_cast<size_t>(step_box.side_edge_x);
        const size_t edge_y = step_box.y + 1;
        canvas.put(right, edge_y, "◀");
        for (size_t x = right + 1; x < edge_x; ++x)
            canvas.put(x, edge_y, "─");
        if (step_box.is_join)
            canvas.put(right + 2, edge_y, fmt::format(" {} ", joinSideLabel(1 - spine_position)));
        canvas.put(edge_x, edge_y, "┐");
        for (size_t y = edge_y + 1; y < side.y; ++y)
            canvas.put(edge_x, y, "│");
        canvas.put(edge_x, side.y, "┴");
    }

    void drawEdges(TextCanvas & canvas, size_t index)
    {
        const auto & step_box = boxes[index];
        if (step_box.children.empty())
            return;

        const auto center = static_cast<size_t>(step_box.center);
        canvas.put(center, step_box.bottom(), "▲");

        std::vector<size_t> in_fragment;
        std::vector<size_t> producers;
        for (size_t child : step_box.children)
            (crossesFragments(index, child) ? producers : in_fragment).push_back(child);

        if (step_box.spine)
        {
            drawStackedEdges(canvas, index);
        }
        else if (!in_fragment.empty())
        {
            canvas.put(center, step_box.bottom() + 1, "│");
            if (in_fragment.size() > 1)
            {
                std::vector<Int64> centers;
                for (size_t child : in_fragment)
                    centers.push_back(boxes[child].center);
                drawBranch(canvas, step_box.bottom() + 2, step_box.center, centers);
            }
            for (size_t child : in_fragment)
                canvas.put(static_cast<size_t>(boxes[child].center), boxes[child].y, "┴");
        }

        if (producers.empty())
            return;

        /// From the receive step down to the frame of its fragment, across it, along the labels, across the frame of
        /// the producer, to its send step.
        const auto & frame = frames[step_box.fragment];
        for (size_t y = step_box.bottom() + 1; y < frame.bottom; ++y)
            canvas.put(center, y, "│");
        canvas.put(center, frame.bottom, "┼");

        /// The fragments of a later band are drawn under the fragments of the earlier bands: the edge passes them, in
        /// the line of its exchange when it goes to one fragment.
        const char * passing_line = "│";
        if (producers.size() == 1
            && graph.exchanges[*boxes[producers.front()].incoming_exchange].mode == ExchangeDescription::Kind::Persisted)
            passing_line = "╎";
        for (size_t y = frame.bottom + 1; y < step_box.producers_y; ++y)
            canvas.put(center, y, passing_line);

        size_t labels_y = step_box.producers_y;
        if (producers.size() > 1)
        {
            std::vector<Int64> centers;
            for (size_t child : producers)
                centers.push_back(boxes[child].center);
            drawBranch(canvas, labels_y, step_box.center, centers);
            ++labels_y;
        }

        for (size_t producer_root : producers)
        {
            const auto & producer = boxes[producer_root];
            const auto x = static_cast<size_t>(producer.center);
            const auto & exchange = graph.exchanges[*producer.incoming_exchange];
            const char * line = exchange.mode == ExchangeDescription::Kind::Persisted ? "╎" : "│";

            const size_t producer_top = producer.y - 2;
            for (size_t y = labels_y; y < producer_top; ++y)
                canvas.put(x, y, line);
            auto lines = labelLines(*producer.incoming_exchange);
            for (size_t i = 0; i < lines.size(); ++i)
                canvas.put(x + 2, labels_y + i, lines[i]);
            canvas.put(x, producer_top, "┼");
            canvas.put(x, producer_top + 1, line);
            canvas.put(x, producer.y, "┴");
        }
    }
};

}

void explainAsTree(
    const FragmentGraph & graph,
    WriteBuffer & buf,
    const ExplainPlanOptions & options,
    size_t max_step_description_length)
{
    const PreparedFragmentGraph prepared(graph, options);

    validateFragmentGraph(graph);

    /// The result columns of the query, as plain `EXPLAIN` prints them before the plan: no box shows them, because
    /// `Expression` steps are not drawn. The fragment that produces the result (`main`) is the first in `print_order`.
    if (options.pretty)
    {
        const QueryFragment & result_fragment
            = prepared.single_node ? graph.fragments.front() : *prepared.print_order.front();
        const QueryPlan & result_plan = PreparedFragmentGraph::getPlan(result_fragment);
        if (!result_plan.getRootNode())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Fragment {} has no plan to draw", result_fragment.id);
        QueryPlanFormat::formatOutputColumns(
            prepared.getPrettyNames(result_plan).pretty_names, buf, *result_plan.getRootNode()->step, /*prefix=*/ "");
        buf.write('\n');
    }

    NestedPlans nested_plans;
    TreeBuilder(prepared, max_step_description_length, nested_plans, prepared.single_node).drawGraph(buf);

    /// Drawing a nested plan can register the nested plans of its own steps: they are appended and drawn in turn.
    for (size_t i = 0; i < nested_plans.size(); ++i)
    {
        /// Copy: drawing appends to `nested_plans`, which invalidates references to its elements.
        const NestedPlan nested_plan = nested_plans[i];
        buf << "\nN" << i + 1 << " · nested plan of " << TextCanvas::replaceControlCharacters(nested_plan.owner) << '\n';
        TreeBuilder(prepared, max_step_description_length, nested_plans, /*frameless=*/ true)
            .drawNestedPlan(*nested_plan.plan, buf);
    }

    /// A plan that was not cut has no exchanges, and its one level says nothing.
    if (prepared.single_node)
    {
        const auto & fragment = graph.fragments.front();
        if (fragment.fallback_reason)
            buf << "\nNot distributed: " << *fragment.fallback_reason << '\n';
        return;
    }

    buf.write('\n');
    writeExchangesSection(prepared, buf);
    writeLevelsSection(graph, buf);
}

}
