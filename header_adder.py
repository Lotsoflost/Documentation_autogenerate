import os
from pathlib import Path, WindowsPath
from typing import Tuple, Optional
from git import Repo, Commit
from git.repo.base import BlameEntry

BASE_PATH = WindowsPath('C:\\Users\\ADMIN\\MyProjects\\Snowflake_task')
GIT_REPO_PATH = BASE_PATH / '.git'


def list_directories(directory_path: Path):
    # This would yield all the directory paths
    for item in directory_path.iterdir():
        if item.is_dir():
            yield item
            yield from list_directories(item)


def list_some_directory():
    # Call the function with the path to your directory
    prepared_callable = list_directories(BASE_PATH)

    print(*list(prepared_callable), sep='\n')


def find_directory_of_file(directory_path: Path, procedure_name: str) -> Tuple[Optional[Path], Optional[Path]]:
    for item in directory_path.iterdir():
        if item.is_file() and item.stem == procedure_name:
            return item.parent, item
        elif item.is_dir():
            if item.name in ('.git',):
               continue
            
            item_parent, item_path = find_directory_of_file(item, procedure_name)
            if item_parent is not None:
                return item_parent, item_path

    return None, None


def analyze_activity(file_name: str):
    """
    Analyze the file name to determine the activity type based on keywords.
    If the name includes 'LOAD' or 'REFRESH', append the next word or suffix to the activity.
    """
    # Define the special suffixes we're looking for after REFRESH or LOAD
    suffixes = {"FCT", "FCTS", "T", "SA", "SRC", "LKP", "WRK", "ML", "MAP", "DIM", "DIMS", "DM", "ALL", "BAD", "LANDING", "MTA", "REF", "STAGING", "TMP", "VALIDATE", "VOD", "VV"}

    file_name_parts = file_name.upper().split('_')

    if "PARSE" in file_name_parts or 'FN' in file_name_parts or "BACKUP" in file_name_parts or "CHANGE" in file_name_parts or "LOGGING" in file_name_parts or "ARRAY" in file_name_parts or "LOG" in file_name_parts:
        return "Utility - Service area" 
    if "GET" in file_name_parts or "EXPORT" in file_name_parts or "CHECK" in file_name_parts or "REPORT" in file_name_parts:
        return "Presenter Layer - Getter API"
    if "PUT" in file_name_parts or "UPDATE" in file_name_parts or "INSERT" in file_name_parts or "DELETE" in file_name_parts or "SAVE" in file_name_parts or "CREATE" in file_name_parts or "COPY" in file_name_parts or "LOCK" in file_name_parts or "SET" in file_name_parts or "INACTIVATE" in file_name_parts or "ADD" in file_name_parts or "UNLOCK" in file_name_parts or "EDIT" in file_name_parts or "DEL" in file_name_parts or "UPD" in file_name_parts or "SAVE" in file_name_parts or "DROP" in file_name_parts:
        return "Presenter Layer - Setter API"
    if "CLEAR" in file_name_parts:
        return "ETL - Clear data area"
    if "SSIS" in file_name_parts:
        return "Utility - Orchestration layer"
    if "RENAME" in file_name_parts:
        return "Utility - Standardization layer"
    if "LOAD" in file_name_parts:
        # Get the next word or suffix after 'LOAD'
        load_index = file_name_parts.index("LOAD")
        next_word_or_suffix = get_next_word_or_suffix(file_name_parts, load_index, suffixes)
        return f"ETL - Load {next_word_or_suffix} layer"
    if "TRANSFER" in file_name_parts:
        return "ETL - Transfer data layer"
    if "VALIDATE" in file_name_parts:
        return "Utility - Standardization layer"
    if "MAP" in file_name_parts or "UNMAP" in file_name_parts or "LINK" in file_name_parts or "UNLINK" in file_name_parts:
        return "ETL - Mapping field"
    if "REFRESH" in file_name_parts:
        # Get the next word or suffix after 'REFRESH'
        refresh_index = file_name_parts.index("REFRESH")
        next_word_or_suffix = get_next_word_or_suffix(file_name_parts, refresh_index, suffixes)
        return f"ETL - Refresh {next_word_or_suffix} layer"

    # Add more conditions as needed
    return "Undefined Activity"


def get_next_word_or_suffix(file_name_parts, index, suffixes):
    """
    Helper function to get the next word or recognized suffix after LOAD or REFRESH.
    It prioritizes valid suffixes (FCT, FCTS, etc.) over other words.
    """
    # Search for the next suffix in the list after LOAD or REFRESH
    for i in range(index + 1, len(file_name_parts)):
        next_word = file_name_parts[i]
        # Check if the word is a suffix we care about (like FCT, FCTS, etc.)
        if next_word in suffixes:
            return next_word
    # If no suffix is found, return the next word (if it exists)
    if index + 1 < len(file_name_parts):
        return file_name_parts[index + 1]

    # If no valid next word exists, return empty
    return ""
def generate_comment_block(directory_path, file_name, author, schema):
    # Extract the base directory name from the BASE_PATH
    project_name = BASE_PATH.name  # This will extract '10_dwh_pat'

    # Determine the activity based on the file name
    activity = analyze_activity(file_name)

    # Check if the file is a function (starts with 'FN_')
    if file_name.startswith("FN_"):
        execution_example = f"SELECT * FROM TABLE({file_name}()) AS my_table;"
    else:
        execution_example = f"CALL {file_name}();"

    comment_block = f"""/*===============================================================================
URL..................: {project_name}\\{directory_path}
Activity.............: {activity}
Description..........:
Owner................: {author}

Execution Example....:
   SET SCHEMA {schema};
   SET PATH = ADMIN, {schema};
   {execution_example}
===============================================================================*/"""
    return comment_block



def inspect_file(repo_obj: Repo, file_path: Path, schema: str):
    blame = repo_obj.blame('HEAD', str(file_path))
    commit, lines = blame[0]  # Assumes the first entry is enough 
    # Get the author's name and apply the transformation
    author_name = commit.author.name

    # Check if the author's name is in lowercase and apply the transformation
    if author_name.islower():
        author_name = author_name[:2].upper() + author_name[2:]  # Capitalize the first 2 letters

    comment_block = generate_comment_block(file_path.parent.relative_to(BASE_PATH), file_path.stem, author_name, schema)

    # read in the file
    with open(file_path, 'r') as file:
        file_data = file.read()

    # prepend the comment block to the content
    file_data = comment_block + '\n' + file_data

    # write the new content back to the file
    with open(file_path, 'w') as file:
        file.write(file_data)
        
    return author_name


def process_files(repo_obj: Repo, base_path: Path, procedures: list) -> list:
    result = []  # List to store the file names and their statuses

    for schema, procedure_name in procedures:
        directory, current_file = find_directory_of_file(base_path, procedure_name)
        record = {'file_name': procedure_name,
                  'state': 'NOT IN GIT', 'author': ''}

        if directory is None:
            result.append(record)
            continue

        if not current_file or not current_file.exists():
            result.append(record)
            continue

        # If the file exists, it's considered 'IN GIT'
        author = inspect_file(repo_obj, current_file, schema)  # Proceed to inspect the file 
        record['author'] = author
        record['state'] = 'IN GIT'
        # if it's in GIT
        result.append(record)

    return result  # Return the list of file names and statuses


# Updated function to convert raw input to schema and procedure names
def prepare_procedure_names(raw_text):
    procedures = []
    # Split the raw input by newline and extract schema and procedure names
    for line in raw_text.strip().splitlines():
        if line.strip():  # Ignore empty lines
            parts = line.split()  # Split by whitespace
            if len(parts) == 2:
                schema, procedure = parts
                # Add schema and procedure name as a tuple
                procedures.append((schema, procedure))

    return procedures


if __name__ == '__main__':
    # Raw procedures input
    raw_procedures = """
    AIR_TEST  SP_REFRESH_DIMS
    AIR_TEST  SP_REFRESH_FCT
    AIR_TEST  SP_UPLOAD_SRC
    ADMIN     SP_ETL_LOG

    """

    # Prepare the list of schema and file names
    procedures = prepare_procedure_names(raw_procedures)

    # Initialize the repo and process the files
    repo = Repo(str(GIT_REPO_PATH)) 
    result = process_files(repo, BASE_PATH, procedures)
    for record in result:
        print(*list(record.values()), sep=', ')