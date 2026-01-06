#!/usr/bin/env python3
import pandas as pd
from pathlib import Path


def remove_put_options_aapl():
    """Remove all PUT options from AAPL data files"""
    
    print("🗑️  Removing PUT options from AAPL data...")
    
    # Process all years (2016-2025)
    years_to_process = list(range(2016, 2026))
    
    total_put_options_removed = 0
    
    for year in years_to_process:
        print(f"\n📅 Processing {year} data...")
        
        # Check both monthly and weekly files
        for period in ['monthly', 'weekly']:
            file_path = Path(f"/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AAPL/{period}/{year}_options_pessimistic.csv")
            
            if not file_path.exists():
                print(f"   ⚠️  {period} file not found: {file_path}")
                continue
            
            try:
                # Load existing data
                df = pd.read_csv(file_path)
                original_count = len(df)
                
                # Count PUT options before removal
                put_count = len(df[df['option_type'] == 'P'])
                call_count = len(df[df['option_type'] == 'C'])
                
                print(f"   📊 {period.capitalize()} file: {original_count} total rows")
                print(f"      CALL options: {call_count}")
                print(f"      PUT options: {put_count}")
                
                # Remove PUT options (keep only CALL options)
                df_calls_only = df[df['option_type'] == 'C'].copy()
                
                # Save the filtered data
                df_calls_only.to_csv(file_path, index=False)
                
                removed_count = original_count - len(df_calls_only)
                total_put_options_removed += removed_count
                
                print(f"   ✅ Removed {removed_count} PUT options")
                print(f"   📄 Saved {len(df_calls_only)} CALL options to {file_path}")
                
            except Exception as e:
                print(f"   ❌ Error processing {year} {period}: {e}")
                import traceback
                traceback.print_exc()
    
    print(f"\n✅ PUT options removal completed!")
    print(f"📊 Summary:")
    print(f"   Total PUT options removed: {total_put_options_removed:,}")
    
    # Final verification
    print(f"\n🔍 Final verification:")
    monthly_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AAPL/monthly")
    weekly_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AAPL/weekly")
    
    for period in ['monthly', 'weekly']:
        dir_path = monthly_dir if period == 'monthly' else weekly_dir
        files = sorted(dir_path.glob("*.csv"))
        
        print(f"\n   {period.capitalize()} files:")
        for f in files:
            year = f.stem.split('_')[0]
            df = pd.read_csv(f)
            call_count = len(df[df['option_type'] == 'C'])
            put_count = len(df[df['option_type'] == 'P'])
            total_count = len(df)
            
            status = "✅" if put_count == 0 else "❌"
            print(f"     {status} {year}: {total_count} total (CALL: {call_count}, PUT: {put_count})")


if __name__ == "__main__":
    remove_put_options_aapl()
