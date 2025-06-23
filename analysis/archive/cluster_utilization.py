import sys
import os
import re
import glob
import csv
import argparse


def extract_utilization_from_stdout(stdout_path):
    """Extract average good utilization and average utilization from stdout file."""
    try:
        with open(stdout_path, 'r') as f:
            content = f.read()
        
        # Extract Average Good Utilization
        good_util_match = re.search(r'Average Good Utilization:\s*([\d\.]+)', content)
        avg_good_util = float(good_util_match.group(1)) if good_util_match else None
        
        # Extract Average Utilization  
        avg_util_match = re.search(r'Average Utilization:\s*([\d\.]+)', content)
        avg_util = float(avg_util_match.group(1)) if avg_util_match else None
        
        return avg_good_util, avg_util
        
    except FileNotFoundError:
        print(f"File not found: {stdout_path}")
        return None, None
    except Exception as e:
        print(f"Error processing {stdout_path}: {e}")
        return None, None


def process_output_directory(output_dir):
    """Process all stdout files in an output directory's analysis subdirectories."""
    results = []
    
    # Find all analysis/*.stdout files in the output directory
    stdout_pattern = os.path.join(output_dir, "**/analysis/*.stdout")
    stdout_files = glob.glob(stdout_pattern, recursive=True)
    
    for stdout_file in stdout_files:
        avg_good_util, avg_util = extract_utilization_from_stdout(stdout_file)
        
        if avg_good_util is not None and avg_util is not None:
            # Extract experiment name from path
            exp_name = os.path.basename(os.path.dirname(os.path.dirname(stdout_file)))
            results.append({
                'output_dir': output_dir,
                'experiment': exp_name,
                'file': stdout_file,
                'avg_good_utilization': avg_good_util,
                'avg_utilization': avg_util
            })
    
    return results


def export_to_csv(results, csv_path):
    """Export results to CSV file."""
    try:
        with open(csv_path, 'w', newline='') as csvfile:
            fieldnames = ['output_dir', 'experiment', 'avg_good_utilization', 'avg_utilization', 'file']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for result in sorted(results, key=lambda x: (x['output_dir'], x['experiment'])):
                writer.writerow(result)
        print(f"Results exported to {csv_path}")
    except Exception as e:
        print(f"Error exporting to CSV: {e}")


def main():
    parser = argparse.ArgumentParser(description='Extract cluster utilization metrics from experiment output directories')
    parser.add_argument('output_dirs', nargs='+', help='One or more output directories to process')
    parser.add_argument('--csv', help='Export results to CSV file')
    parser.add_argument('--quiet', '-q', action='store_true', help='Suppress detailed output')
    
    args = parser.parse_args()
    
    all_results = []
    
    for output_dir in args.output_dirs:
        if not os.path.exists(output_dir):
            if not args.quiet:
                print(f"Warning: Directory {output_dir} does not exist")
            continue
            
        if not args.quiet:
            print(f"Processing output directory: {output_dir}")
        results = process_output_directory(output_dir)
        all_results.extend(results)
        
        if not args.quiet:
            print(f"Found {len(results)} experiments in {output_dir}")
    
    if not all_results:
        print("No experiments found in any of the provided directories")
        return
    
    # Export to CSV if requested
    if args.csv:
        export_to_csv(all_results, args.csv)
    
    # Print table unless quiet mode
    if not args.quiet:
        print(f"{'Output Directory':<30} {'Experiment':<50} {'Avg Good Util':<15} {'Avg Util':<15}")
        print("-" * 110)
        
        for result in sorted(all_results, key=lambda x: (x['output_dir'], x['experiment'])):
            print(f"{result['output_dir']:<30} {result['experiment']:<50} {result['avg_good_utilization']:<15.2f} {result['avg_utilization']:<15.2f}")


if __name__ == "__main__":
    main()