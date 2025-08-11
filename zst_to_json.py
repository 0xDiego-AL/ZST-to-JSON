import os
import zstandard as zstd
import json
import io

# Define input and output folders based on the script's directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FOLDER = SCRIPT_DIR  # Folder containing .zst files
OUTPUT_FOLDER = SCRIPT_DIR  # Folder to save JSON files

# Ensure the output folder exists (SCRIPT_DIR should already exist; safe to call)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def decompress_and_validate_zst_streaming(input_zst, output_json):
    """ Decompresses a .zst file and validates/cleans JSON lines using streaming for memory efficiency """
    valid_lines = 0
    invalid_lines = 0
    
    print(f"   🔄 Processing {os.path.basename(input_zst)}...")
    
    with open(input_zst, 'rb') as compressed_file:
        dctx = zstd.ZstdDecompressor()
        
        with open(output_json, 'w', encoding='utf-8') as output_file:
            # Use streaming decompression with a text wrapper
            with dctx.stream_reader(compressed_file) as stream_reader:
                text_stream = io.TextIOWrapper(stream_reader, encoding='utf-8')
                
                line_buffer = ""
                chunk_size = 0
                
                for chunk in text_stream:
                    chunk_size += len(chunk)
                    line_buffer += chunk
                    
                    # Process complete lines
                    while '\n' in line_buffer:
                        line, line_buffer = line_buffer.split('\n', 1)
                        line = line.strip()
                        
                        # Skip empty lines
                        if not line:
                            continue
                        
                        try:
                            # Try to parse the JSON to validate it
                            json_obj = json.loads(line)
                            
                            # Write the valid JSON line
                            output_file.write(line + '\n')
                            valid_lines += 1
                            
                            # Progress indicator every 10000 lines
                            if valid_lines % 10000 == 0:
                                print(f"   📊 Processed {valid_lines:,} valid lines...")
                            
                        except json.JSONDecodeError:
                            invalid_lines += 1
                            # Only show first few errors to avoid spam
                            if invalid_lines <= 5:
                                print(f"   ⚠️  Skipping malformed JSON line (total skipped: {invalid_lines})")
                        except Exception:
                            invalid_lines += 1
                            if invalid_lines <= 5:
                                print(f"   ⚠️  Skipping problematic line (total skipped: {invalid_lines})")
                
                # Process any remaining content in buffer
                if line_buffer.strip():
                    try:
                        json_obj = json.loads(line_buffer.strip())
                        output_file.write(line_buffer.strip() + '\n')
                        valid_lines += 1
                    except:
                        invalid_lines += 1
    
    return valid_lines, invalid_lines

# Find all .zst files in the script's directory
zst_files = [f for f in os.listdir(INPUT_FOLDER) if f.endswith('.zst')]

if not zst_files:
    print("❌ No .zst files found in the folder!")
    print(f"   📁 Looking in: {INPUT_FOLDER}")
else:
    print(f"🔄 Found {len(zst_files)} .zst files. Starting conversion...\n")
    
    total_valid = 0
    total_invalid = 0

    for i, file in enumerate(zst_files, 1):
        input_path = os.path.join(INPUT_FOLDER, file)
        output_path = os.path.join(OUTPUT_FOLDER, file.replace(".zst", ".json"))

        print(f"📂 Converting {i}/{len(zst_files)}: {file}")
        
        try:
            valid_lines, invalid_lines = decompress_and_validate_zst_streaming(input_path, output_path)
            total_valid += valid_lines
            total_invalid += invalid_lines
            
            print(f"✅ Done: {file}")
            print(f"   📊 Valid lines: {valid_lines:,}, Invalid lines: {invalid_lines:,}\n")
            
        except Exception as e:
            print(f"❌ Error processing {file}: {str(e)}")
            continue

    print(f"🎉 Conversion complete!")
    print(f"📈 Total valid lines: {total_valid:,}")
    print(f"⚠️  Total invalid lines: {total_invalid:,}")
    print(f"📁 JSON files saved in: {OUTPUT_FOLDER}")
    print(f"\n💡 These files are now ready for PySpark with spark.read.json()")
