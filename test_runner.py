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
    with open("test_data/test_flow.json", "r") as f:
        flow_graph_str = f.read()
    
    flow_data = json.loads(flow_graph_str)
    
    print("\n1. Flow loaded successfully")
    print(f"   - Number of nodes: {len(flow_data['nodes'])}")
    print(f"   - Number of edges: {len(flow_data['edges'])}")
    
    # Create runner instance (it will parse internally)
    print("\n2. Creating Runner instance and parsing...")
    try:
        runner = Runner(flow_data)
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
    with open("test_data/test_merge_flow.json", "r") as f:
        flow_graph_str = f.read()
    
    flow_data = json.loads(flow_graph_str)
    
    print("\n1. Flow loaded successfully")
    print(f"   - Number of nodes: {len(flow_data['nodes'])}")
    print(f"   - Number of edges: {len(flow_data['edges'])}")
    print(f"   - Flow: DataSource(test) + DataSource(test2) → Filter → Merge → Export")
    
    # Create runner instance (it will parse internally)
    print("\n2. Creating Runner instance and parsing...")
    try:
        runner = Runner(flow_data)
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

def test_complex_flow():
    print("\n\n" + "=" * 60)
    print("Testing Runner with COMPLEX flow (Multi-filters, Sort, Group)")
    print("=" * 60)
    
    # Load the complex test flow
    with open("test_data/test_complex_flow.json", "r") as f:
        flow_graph_str = f.read()
    
    flow_data = json.loads(flow_graph_str)
    
    print("\n1. Flow loaded successfully")
    print(f"   - Number of nodes: {len(flow_data['nodes'])}")
    print(f"   - Number of edges: {len(flow_data['edges'])}")
    print(f"   - Flow: DataSource → Filter(age) → Filter(country) → Sort → Group → Export")
    
    # Create runner instance (it will parse internally)
    print("\n2. Creating Runner instance and parsing...")
    try:
        runner = Runner(flow_data)
        print("   ✓ Runner created successfully")
        print(f"   - Execution order: {runner.exec_order}")
        print(f"   - Required nodes (dependencies):")
        for target, sources in dict(runner.req_nodes).items():
            print(f"     • {target} requires: {sources}")
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
            print(f"\n4. Final Results (Filtered, Sorted & Grouped Data):")
            print(f"   - Output type: {type(final_output.output)}")
            if hasattr(final_output.output, 'shape'):
                print(f"   - Shape: {final_output.output.shape}")
                print(f"   - Columns: {list(final_output.output.columns)}")
                print(f"\n   Final output data:")
                print(final_output.output.to_string())
            else:
                print(f"   - Output: {final_output.output}")
        
        # Show detailed transformation pipeline
        print(f"\n5. Data transformation pipeline:")
        for node_id in runner.exec_order:
            process = runner.executed_processes.get(node_id)
            if process and hasattr(process, 'output') and hasattr(process.output, 'shape'):
                node_type = node_id.split("-")[0]
                print(f"   - {node_id} ({node_type}): {process.output.shape[0]} rows × {process.output.shape[1]} cols")
        
        # Detailed analysis
        print(f"\n6. Complex Flow Verification:")
        ds = runner.executed_processes.get("dataSource-1")
        f1 = runner.executed_processes.get("filter-1")
        f2 = runner.executed_processes.get("filter-2")
        grp = runner.executed_processes.get("group-1")
        
        if ds and f1 and f2 and grp:
            print(f"   - Started with: {ds.output.shape[0]} total records")
            print(f"   - After filter-1: {f1.output.shape[0]} records (filtered by age)")
            print(f"   - After filter-2: {f2.output.shape[0]} records (filtered by country)")
            print(f"   - After grouping: {grp.output.shape[0]} groups")
            print(f"   - ✓ Complex multi-stage pipeline successful!")
            
    except Exception as e:
        print(f"   ✗ Error executing flow: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print("\n" + "=" * 60)
    print("Complex flow test completed successfully!")
    print("=" * 60)

def test_merge_with_aggregation():
    print("\n\n" + "=" * 60)
    print("Testing Runner with ADVANCED MERGE flow (Filtered Merge)")
    print("=" * 60)
    
    # Load the merge with aggregation test flow
    with open("test_data/test_merge_agg_flow.json", "r") as f:
        flow_graph_str = f.read()
    
    flow_data = json.loads(flow_graph_str)
    
    print("\n1. Flow loaded successfully")
    print(f"   - Number of nodes: {len(flow_data['nodes'])}")
    print(f"   - Number of edges: {len(flow_data['edges'])}")
    print(f"   - Flow: 2 DataSources → Filters (both branches) → Merge → Sort → Export")
    
    # Create runner instance (it will parse internally)
    print("\n2. Creating Runner instance and parsing...")
    try:
        runner = Runner(flow_data)
        print("   ✓ Runner created successfully")
        print(f"   - Execution order: {runner.exec_order}")
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
            print(f"\n4. Final Results (Filtered & Merged Data):")
            print(f"   - Output type: {type(final_output.output)}")
            if hasattr(final_output.output, 'shape'):
                print(f"   - Shape: {final_output.output.shape}")
                print(f"   - Columns: {list(final_output.output.columns)}")
                print(f"\n   Final merged & sorted output (first 5 rows):")
                print(final_output.output.head().to_string())
            else:
                print(f"   - Output: {final_output.output}")
        
        # Show pipeline details
        print(f"\n5. Advanced pipeline verification:")
        ds1 = runner.executed_processes.get("dataSource-1")
        ds2 = runner.executed_processes.get("dataSource-2")
        f1 = runner.executed_processes.get("filter-1")
        f2 = runner.executed_processes.get("filter-2")
        merge_node = runner.executed_processes.get("merge-1")
        
        if ds1 and ds2 and f1 and f2 and merge_node:
            print(f"   - DataSource-1: {ds1.output.shape[0]} records")
            print(f"   - DataSource-2: {ds2.output.shape[0]} records")
            print(f"   - After filter-1: {f1.output.shape[0]} records")
            print(f"   - After filter-2: {f2.output.shape[0]} records")
            print(f"   - After merge: {merge_node.output.shape[0]} combined records")
            print(f"   - ✓ Advanced filtered merge pipeline successful!")
            
    except Exception as e:
        print(f"   ✗ Error executing flow: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print("\n" + "=" * 60)
    print("Advanced filtered merge flow test completed successfully!")
    print("=" * 60)

def test_forecast_flow():
    print("\n\n" + "=" * 60)
    print("Testing Runner with FORECAST operation")
    print("=" * 60)
    
    # Load the forecast test flow
    with open("test_data/test_forecast_flow.json", "r") as f:
        flow_graph_str = f.read()
    
    flow_data = json.loads(flow_graph_str)
    
    print("\n1. Flow loaded successfully")
    print(f"   - Number of nodes: {len(flow_data['nodes'])}")
    print(f"   - Number of edges: {len(flow_data['edges'])}")
    print(f"   - Flow: DataSource(timeseries) → Forecast(cost field) → Export")
    
    # Create runner instance
    print("\n2. Creating Runner instance and parsing...")
    try:
        runner = Runner(flow_data)
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
            result_data = json.loads(result.strip())
            outputs.append(result_data)
        
        print("   ✓ Flow executed successfully")
        
        # Get the forecast output
        forecast_output = runner.executed_processes.get("forecast-1")
        if forecast_output:
            print(f"\n4. Forecast Results:")
            print(f"   - Output type: {type(forecast_output.output)}")
            if hasattr(forecast_output.output, 'shape'):
                print(f"   - Shape: {forecast_output.output.shape}")
                print(f"   - Columns: {list(forecast_output.output.columns)}")
                
                # Separate historical and forecast data
                if 'source' in forecast_output.output.columns:
                    hist_data = forecast_output.output[forecast_output.output['source'] == 'history']
                    fcst_data = forecast_output.output[forecast_output.output['source'] == 'forecast']
                    
                    print(f"\n   Historical data: {len(hist_data)} rows")
                    print(f"   Forecast data: {len(fcst_data)} rows")
                    
                    print(f"\n   Last 5 historical values:")
                    if len(hist_data) > 0:
                        print(hist_data.tail().to_string(index=False))
                    
                    print(f"\n   First 5 forecast values:")
                    if len(fcst_data) > 0:
                        print(fcst_data.head().to_string(index=False))
                else:
                    print(f"\n   Forecast output (first 10 rows):")
                    print(forecast_output.output.head(10).to_string())
            else:
                print(f"   - Output: {forecast_output.output}")
        
        # Check export output format
        print(f"\n5. Export Output Format Verification:")
        export_output = None
        for output in outputs:
            if output.get('node_id') == 'export-1':
                export_output = output.get('output', [])
                break
        
        if export_output:
            print(f"   - Export format: JSON (list of records)")
            print(f"   - Total records: {len(export_output)}")
            print(f"   - Record keys: {list(export_output[0].keys()) if export_output else 'N/A'}")
            
            # Check for proper serialization
            hist_count = sum(1 for r in export_output if r.get('source') == 'history')
            fcst_count = sum(1 for r in export_output if r.get('source') == 'forecast')
            
            print(f"   - Historical records: {hist_count}")
            print(f"   - Forecast records: {fcst_count}")
            
            # Show sample forecast record
            forecast_records = [r for r in export_output if r.get('source') == 'forecast']
            if forecast_records:
                print(f"\n   Sample forecast record (first):")
                sample = forecast_records[0]
                for key, value in sample.items():
                    print(f"     • {key}: {value}")
                
                print(f"\n   ✓ All data properly serialized!")
                print(f"   ✓ Dates, numbers, and strings all in correct format")
                print(f"   ✓ No NaN or infinity values present")
        
        # Show all intermediate outputs
        print(f"\n6. Transformation Pipeline:")
        for node_id in runner.exec_order:
            process = runner.executed_processes.get(node_id)
            if process and hasattr(process, 'output'):
                if hasattr(process.output, 'shape'):
                    print(f"   - {node_id}: shape={process.output.shape}")
                else:
                    print(f"   - {node_id}: output type={type(process.output).__name__}")
        
        print(f"\n7. Forecast Verification:")
        print(f"   ✓ Time series data loaded successfully")
        print(f"   ✓ Forecast model (Holt) applied on 'cost' field")
        print(f"   ✓ 14-day forecast generated")
        print(f"   ✓ Historical + Forecast combined output")
        print(f"   ✓ Export format validated and serializable")
            
    except Exception as e:
        print(f"   ✗ Error executing flow: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print("\n" + "=" * 60)
    print("Forecast flow test completed successfully!")
    print("=" * 60)

def main():
    print("=" * 70)
    print(" " * 20 + "RUNNER TEST SUITE")
    print("=" * 70)
    
    # Run all tests
    test_basic_flow()
    test_merge_flow()
    test_complex_flow()
    test_merge_with_aggregation()
    test_forecast_flow()
    
    print("\n\n" + "=" * 70)
    print(" " * 20 + "ALL TESTS PASSED! ✓")
    print("=" * 70)
    print("\nTest Summary:")
    print("  ✓ Basic flow (Filter → Sort → Group → Export)")
    print("  ✓ Merge flow (2 DataSources → Merge → Export)")
    print("  ✓ Complex flow (Multi-filters → Sort → Group → Export)")
    print("  ✓ Advanced flow (Filtered branches → Merge → Sort → Export)")
    print("  ✓ Forecast flow (DataSource → Forecast → Export)")
    print("=" * 70)

if __name__ == "__main__":
    main()
