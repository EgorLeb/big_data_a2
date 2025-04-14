import sys


for line in sys.stdin:
    try:
        doc_id, title, text = line.strip().split('\t')
        doc_length = len(text.split())
        print(f"{doc_id}\t{doc_length}")
    except Exception as e:
        print(f"Error processing line: ERROR: {str(e)}", file=sys.stderr)
