#!/usr/bin/env python3
import os, sys, subprocess, time, argparse

scripts = [
    ("create_us_daily_prices.py", "Data Collection", 600),
    ("smart_money_screener_v2.py", "Screening", 600),
    ("sector_heatmap.py", "Heatmap", 300),
    ("options_flow.py", "Options", 300),
    ("ai_summary_generator.py", "AI summaries", 900),
    ("final_report_generator.py", "Final Report", 60),
    ("macro_analyzer.py", "Macro Analysis", 300),
    ("economic_calendar.py", "Calendar", 300)
]

def run_script(name, desc, timeout):
    print(f"Running {desc}...")
    try:
        subprocess.run([sys.executable, name], timeout=timeout, check=True)
        print("✅ Done")
    except Exception as e:
        print(f"❌ Failed: {e}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--quick', action='store_true')
    args = parser.parse_args()
    
    start = time.time()
    for name, desc, timeout in scripts:
        if args.quick and "AI" in desc: continue
        run_script(name, desc, timeout)
        
    print(f"Total time: {(time.time()-start)/60:.1f} min")

if __name__ == "__main__":
    main()
