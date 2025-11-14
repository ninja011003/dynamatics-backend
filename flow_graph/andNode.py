import pandas as pd
from typing import List


class And:
    """
    AND Node: Combines multiple filter nodes using logical AND.
    Only rows that pass ALL filter conditions will be included.
    Assumes all input nodes are Filter nodes with computed masks.
    """
    def __init__(self, *filter_nodes):
        """
        Args:
            *filter_nodes: Variable number of Filter node instances
        """
        if len(filter_nodes) < 2:
            raise ValueError("AND node requires at least 2 filter nodes")
        
        # Use the first filter's input as the base DataFrame
        self.input = filter_nodes[0].input
        self.filter_nodes = filter_nodes
        self._output = None
        self._computed = False
        self.mask = None

    def run(self):
        """
        Combines masks from all filter nodes using logical AND.
        The actual filtering is deferred until output is accessed.
        """
        # Combine all masks using AND operation
        combined_mask = self.filter_nodes[0].mask
        
        for filter_node in self.filter_nodes[1:]:
            if filter_node.mask is None:
                raise ValueError("Filter node has no mask. Ensure run() was called on all filter nodes.")
            combined_mask = combined_mask & filter_node.mask
        
        self.mask = combined_mask
        self._computed = False  # Reset computation flag
        return self

    @property
    def output(self):
        """Lazy evaluation: apply combined mask only when accessed"""
        if not self._computed:
            if self.mask is None:
                self._output = self.input
            else:
                self._output = self.input[self.mask].reset_index(drop=True)
            self._computed = True
        return self._output


if __name__ == "__main__":
    from filter import Filter
    
    # Example: Find rows where A is in range [3,20] AND B > 3
    df = pd.DataFrame({
        "A": [1, 2, 3, 5, 3, 24, 7, 8],
        "B": [1, 2, 3, 4, 5, 6, 7, 8]
    })
    
    # Create two filter nodes
    filter1 = Filter(df)
    filter1.run(field="A", condition="range", value1=3, value2=20)
    
    filter2 = Filter(df)
    filter2.run(field="B", condition="gt", value1=3)
    
    # Combine with AND
    and_node = And(filter1, filter2)
    and_node.run()
    
    # Output is computed lazily only when accessed
    print("Combined result (A in [3,20] AND B > 3):")
    print(and_node.output)
    print(f"\nMask: {and_node.mask.tolist()}")
