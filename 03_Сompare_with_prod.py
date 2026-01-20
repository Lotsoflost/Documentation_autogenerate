import re
import os
import shutil


# This script normalizes the text of procedures and compares prod and repo versions

def remove_comments(content: str) -> str:
    """
    Remove comments from the SQL content.
    - Removes multi-line comments (/* ... */)
    - Removes single-line comments (-- ...)
    """
    # Remove multi-line comments: /* ... */
    content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)

    # Remove single-line comments: -- ...
    content = re.sub(r'--.*', '', content)

    return content

def normalize_file_content(file_path):
    """
    Read a file and normalize its content by:
    - Removing extra spaces, tabs, and empty lines
    - Removing comments
    - Handling encoding errors gracefully by trying different encodings
    - Normalizing END and END; to be treated the same check SP_PUT_POSTING_LOG
    - Ignoring specific keywords: PAT_APP_TEST_ASOKOLOV and PAT_APP (case-insensitive)
    """
    encodings = ['utf-8', 'utf-16', 'latin-1', 'cp1252']  # List of encodings to try
    content = None

    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as file:
                lines = file.readlines()
                # Normalize: Strip spaces and tabs, remove empty lines
                normalized_lines = [line.strip() for line in lines if line.strip()]
                # Join the lines into a single content string
                content = "\n".join(normalized_lines)
                # Remove comments
                content = remove_comments(content)
                # Collapse multiple spaces into a single space
                content = re.sub(r'[^\S\r\n]+', '', content)
                # Normalize END and END; to treat them as the same
                content = re.sub(r'\bEND\b\s*;', 'END', content)
                content = content.replace("COGNOS_REPORTS", "")
                # Remove PAT_APP_TEST_ASOKOLOV and PAT_APP (case-insensitive)
                content = re.sub(r'(?i)\bPAT_APP_TEST_ASOKOLOV\b', '', content)
                content = re.sub(r'(?i)\bPAT_APP\b', '', content)
                # Join the lines into a single content string
                content = "\n".join([line.strip() for line in content.split('\n') if line.strip()])
                break  # Stop trying encodings if successful
        except (UnicodeDecodeError, FileNotFoundError, UnicodeError):
            continue  # Try the next encoding if there's an error

    if content is None:
        return ""  # Return an empty string if no encoding works

    return content

def should_skip_directory(directory_name):
    """
    Check if a directory should be skipped based on the presence of restricted keywords.
    """
    restricted_keywords = ['_not_found', '_unused', 'bkp_', 'release_20', 'save_from_', 'old', 'not_used', 'activate quarter', 'release', 'musor']
    return any(keyword in directory_name for keyword in restricted_keywords)

def compare_and_replace_files(new_files_dir, target_dir):
    result = []  # To store the list of files and their statuses

    # Walk through the new files in the save_from_prod directory
    for root, _, files in os.walk(new_files_dir):
        for file_name in files:
            new_file_path = os.path.join(root, file_name)
            new_file_name_lower = file_name.lower()

            # Search for the file with the same name in the target directory
            for subdir_root, _, subdir_files in os.walk(target_dir):
                # Skip directories that match restricted keywords
                if should_skip_directory(subdir_root):
                    continue

                subdir_files_lower = [f.lower() for f in subdir_files]

                if new_file_name_lower in subdir_files_lower:
                    matching_file_index = subdir_files_lower.index(new_file_name_lower)
                    old_file_name = subdir_files[matching_file_index]
                    old_file_path = os.path.join(subdir_root, old_file_name)

                    # Normalize the contents of both files for comparison
                    new_file_content = normalize_file_content(new_file_path)
                    old_file_content = normalize_file_content(old_file_path)

                    # Compare the normalized content
                    if new_file_content != old_file_content:
                        # If the files are different after normalization, replace the old file with the new one
                        shutil.copyfile(new_file_path, old_file_path)
                        result.append({
                            'file_name': old_file_name,
                            'directory': subdir_root,
                            'status': 'TAKEN FROM PROD'
                        })
                    else:
                        # Files are considered the same after normalization
                        result.append({
                            'file_name': old_file_name,
                            'directory': subdir_root,
                            'status': 'SAME'
                        })

    return result

if __name__ == '__main__':
    # Example usage
    new_files_dir = r'C:\\Users\\ADMIN\\MyProjects\\Snowflake_task\\save_from_snowflake'
    target_dir = r'C:\\Users\\ADMIN\\MyProjects\\Snowflake_task'
    
    # Compare and replace old files with new ones
    files_status = compare_and_replace_files(new_files_dir, target_dir)
    
    # Print the result
    for file_info in files_status:
        print(f"File: {file_info['file_name']}, Directory: {file_info['directory']}, Status: {file_info['status']}")
