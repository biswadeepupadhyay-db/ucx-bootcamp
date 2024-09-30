"""util functions for deploy_init.py"""
import os
import sys
import json
import random
import string
import datetime
import functools
import subprocess
from typing import Optional


class DatabricksException(Exception):
    """Raise this for any custom exceptions."""


def generate_random_string(length):
    """Generate a random string of given length."""
    letters = string.ascii_letters + string.digits
    return ''.join(random.choice(letters) for _ in range(length))


def exec_command(command):
    """Executes a given command and returns its output and command status."""
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        return False, e.stderr


def read_json_file(file_path: str) -> dict:
    """
    Reads a JSON file and returns its contents as a Python object.
    :param file_path: The path to the JSON file
    :return: The contents of the JSON file as a Python Dictionary
    """
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        print(f"Writing deploystate at {file_path}")
        return {}
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in file {file_path}")
        print(f"JSON decode error: {str(e)}")
        raise


def write_deploy_state(data, path):
    """
    writes deploy state data to file
    :param data: deploys tate data to write
    :param path: deploy state file path
    :return: None
    """
    try:
        with open(path, 'w') as json_file:
            json.dump(data, json_file)
        return True
    except Exception as e:
        print(f"Failed to store deployment state with the error: {e}\nState could not be persisted for restart.")
        return False


def write_json_file(data, path):
    """
    writes json data to file
    :param data: json data to write
    :param path: target file path
    :return: None
    """
    try:
        with open(path, 'w') as json_file:
            json.dump(data, json_file)
        return True
    except Exception as e:
        print(f"Failed to write ws deployment config data: {e}.")
        return False


def write_tfvars(variables, file_path):
    """Creates tfvars file for terraform deployment."""
    try:
        if not os.path.exists(file_path):
            os.remove(file_path)
        with open(file_path, 'w') as f:
            for key, value in variables.items():
                if isinstance(value, str):
                    f.write(f'{key} = "{value}"\n')
                else:
                    f.write(f'{key} = {value}\n')
        return True
    except Exception as e:
        raise DatabricksException("An error occurred while creating tfvars.") from e



def ensure_input(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        def get_input(prompt):
            while True:
                value = input(prompt)
                if value.strip():
                    return value
                print("This input is mandatory. Please enter a value.")

        # Replace the built-in input function with our custom get_input function
        original_input = __builtins__.input
        __builtins__.input = get_input

        try:
            return func(*args, **kwargs)
        finally:
            # Restore the original input function
            __builtins__.input = original_input

    return wrapper


def get_full_name_from_email(email):
    # Extract the part before '@'
    name_part = email.split('@')[0]

    # Check if there's a dot in the name part
    if '.' in name_part:
        # Split by dot and capitalize each part
        parts = name_part.split('.')
        full_name = ' '.join(part.capitalize() for part in parts)
    else:
        # If no dot, just capitalize the first letter
        full_name = name_part.capitalize()

    return full_name


