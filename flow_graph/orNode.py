import pandas as pd
from typing import List


class Or:
    """
    OR Node: Combines multiple filter nodes using logical OR.
    Rows that pass ANY filter condition will be included.
    Assumes all input nodes are Filter nodes with computed masks.
    """
    def __init__(self, *filter_nodes):
        """
        Args:
            *filter_nodes: Variable number of Filter node instances
        """
        if len(filter_nodes) < 2:
            raise ValueError("OR node requires at least 2 filter nodes")
        
        # Use the first filter's input as the base DataFrame
        self.input = filter_nodes[0].input
        self.filter_nodes = filter_nodes
        self._output = None
        self._computed = False
        self.mask = None

    def run(self):
        """
        Combines masks from all filter nodes using logical OR.
        The actual filtering is deferred until output is accessed.
        """
        # Combine all masks using OR operation
        combined_mask = self.filter_nodes[0].mask
        
        for filter_node in self.filter_nodes[1:]:
            if filter_node.mask is None:
                raise ValueError("Filter node has no mask. Ensure run() was called on all filter nodes.")
            combined_mask = combined_mask | filter_node.mask
        
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
    
    # Example: Find rows where A < 3 OR B > 6
    df = pd.DataFrame({
        "A": [1, 2, 3, 5, 3, 24, 7, 8],
        "B": [1, 2, 3, 4, 5, 6, 7, 8]
    })
    
    # Create two filter nodes
    filter1 = Filter(df)
    filter1.run(field="A", condition="lt", value1=3)
    
    filter2 = Filter(df)
    filter2.run(field="B", condition="gt", value1=6)
    
    # Combine with OR
    or_node = Or(filter1, filter2)
    or_node.run()
    
    # Output is computed lazily only when accessed
    print("Combined result (A < 3 OR B > 6):")
    print(or_node.output)
    print(f"\nMask: {or_node.mask.tolist()}")

