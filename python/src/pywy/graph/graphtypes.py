from typing import Iterable, List, Tuple

from pywy.graph.graph import GraphNode, WayangGraph
from pywy.wayangplan.base import WyOperator

class NodeOperator(GraphNode[WyOperator]):

    def __init__(self, op: WyOperator):
        super(NodeOperator, self).__init__(op)

    def getadjacents(self) -> Iterable[WyOperator]:
        operator: WyOperator = self.current
        if operator is None or operator.inputs == 0:
            return []
        return operator.inputOperator

    def build_node(self, t:WyOperator) -> 'NodeOperator':
        return NodeOperator(t)

class WGraphOfOperator(WayangGraph[NodeOperator]):

    def __init__(self, nodes: List[WyOperator]):
        super(WGraphOfOperator, self).__init__(nodes)

    def build_node(self, t:WyOperator) -> NodeOperator:
        return NodeOperator(t)


class NodeTuple(GraphNode[Tuple[WyOperator, WyOperator]]):

    def __init__(self, op: WyOperator):
        super(NodeTuple, self).__init__((op, None))

    def getadjacents(self) -> Iterable[Tuple[WyOperator, WyOperator]]:
        operator: WyOperator = self.current[0]
        if operator is None or operator.inputs == 0:
            return []
        return operator.inputOperator

    def build_node(self, t:WyOperator) -> 'NodeTuple':
        return NodeTuple(t)

class WGraphOfTuple(WayangGraph[NodeTuple]):

    def __init__(self, nodes: List[WyOperator]):
        super(WGraphOfTuple, self).__init__(nodes)

    def build_node(self, t:WyOperator) -> NodeTuple:
        return NodeTuple(t)