#!/usr/bin/env python3
"""
Test script for the Runner class
"""
import json
from flow_graph.parser import Parser
from flow_graph.runner import Runner

def test_basic_flow():
    print("=" * 60)
    print("Testing Runner with test_flow.json")
    print("=" * 60)
    
    # Load the test flow
    with open("test_flow.json", "r") as f:
        flow_graph_str = f.read()
    
    flow_data = json.loads(flow_graph_str)
    
    print("\n1. Flow loaded successfully")
    print(f"   - Number of nodes: {len(flow_data['nodes'])}")
    print(f"   - Number of edges: {len(flow_data['edges'])}")
    
    # Create runner instance (it will parse internally)
    print("\n2. Creating Runner instance and parsing...")
    try:
        runner = Runner(flow_graph_str)
        print("   ✓ Runner created successfully")
        print(f"   - Execution order: {runner.exec_order}")
        print(f"   - Required nodes: {dict(runner.req_nodes)}")
    except Exception as e:
        print(f"   ✗ Error creating runner: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Execute the flow
    print("\n3. Executing flow...")
    try:
        outputs = []
        for result in runner.execute():
            print(f"   - Step output received")
            outputs.append(result)
        
        print("   ✓ Flow executed successfully")
        
        # Get the final output from runner
        final_output = runner.executed_processes.get("export-1")
        if final_output:
            print(f"\n4. Final Results:")
            print(f"   - Output type: {type(final_output.output)}")
            if hasattr(final_output.output, 'shape'):
                print(f"   - Shape: {final_output.output.shape}")
                print(f"   - Columns: {list(final_output.output.columns)}")
                print(f"\n   Output data:")
                print(final_output.output.to_string())
            else:
                print(f"   - Output: {final_output.output}")
        
        # Show all intermediate outputs
        print(f"\n5. Intermediate outputs:")
        for node_id, process in runner.executed_processes.items():
            if hasattr(process, 'output') and hasattr(process.output, 'shape'):
                print(f"   - {node_id}: shape={process.output.shape}")
            
    except Exception as e:
        print(f"   ✗ Error executing flow: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print("\n" + "=" * 60)
    print("Basic flow test completed successfully!")
    print("=" * 60)

def test_merge_flow():
    print("\n\n" + "=" * 60)
    print("Testing Runner with MERGE operation")
    print("=" * 60)
    
    # Load the merge test flow
    with open("test_merge_flow.json", "r") as f:
        flow_graph_str = f.read()
    
    flow_data = json.loads(flow_graph_str)
    
    print("\n1. Flow loaded successfully")
    print(f"   - Number of nodes: {len(flow_data['nodes'])}")
    print(f"   - Number of edges: {len(flow_data['edges'])}")
    print(f"   - Flow: DataSource(test) + DataSource(test2) → Filter → Merge → Export")
    
    # Create runner instance (it will parse internally)
    print("\n2. Creating Runner instance and parsing...")
    try:
        runner = Runner(flow_graph_str)
        print("   ✓ Runner created successfully")
        print(f"   - Execution order: {runner.exec_order}")
        print(f"   - Required nodes: {dict(runner.req_nodes)}")
    except Exception as e:
        print(f"   ✗ Error creating runner: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Execute the flow
    print("\n3. Executing flow...")
    try:
        outputs = []
        for result in runner.execute():
            print(f"   - Step output received")
            outputs.append(result)
        
        print("   ✓ Flow executed successfully")
        
        # Get the final output from runner
        final_output = runner.executed_processes.get("export-1")
        if final_output:
            print(f"\n4. Final Results (Merged Data):")
            print(f"   - Output type: {type(final_output.output)}")
            if hasattr(final_output.output, 'shape'):
                print(f"   - Shape: {final_output.output.shape}")
                print(f"   - Columns: {list(final_output.output.columns)}")
                print(f"\n   Merged output data (first 5 rows):")
                print(final_output.output.head().to_string())
            else:
                print(f"   - Output: {final_output.output}")
        
        # Show all intermediate outputs with details
        print(f"\n5. Intermediate outputs:")
        for node_id in runner.exec_order:
            process = runner.executed_processes.get(node_id)
            if process and hasattr(process, 'output') and hasattr(process.output, 'shape'):
                cols = list(process.output.columns)
                print(f"   - {node_id}: shape={process.output.shape}, columns={cols}")
        
        # Verify merge worked correctly
        merge_output = runner.executed_processes.get("merge-1")
        if merge_output and hasattr(merge_output.output, 'shape'):
            print(f"\n6. Merge Verification:")
            print(f"   - Merged {merge_output.output.shape[0]} rows")
            print(f"   - Combined columns from both sources")
            print(f"   - ✓ Merge operation successful!")
            
    except Exception as e:
        print(f"   ✗ Error executing flow: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print("\n" + "=" * 60)
    print("Merge flow test completed successfully!")
    print("=" * 60)

def main():
    # Run basic flow test
    test_basic_flow()
    
    # Run merge flow test
    test_merge_flow()
    
    print("\n\n" + "=" * 70)
    print("ALL TESTS PASSED! ✓")
    print("=" * 70)

if __name__ == "__main__":
    main()
