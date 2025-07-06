"""
Utility module for running DBT commands and managing DBT projects.
"""

import os
import subprocess
import json
from src.utils.logger import setup_logger

# Set up logger
logger = setup_logger(__name__)

class DBTRunner:
    """
    Class for running DBT commands and managing DBT projects.
    """
    
    def __init__(self, project_dir=None, profiles_dir=None):
        """
        Initialize the DBTRunner.
        
        Args:
            project_dir (str, optional): Path to the DBT project directory
            profiles_dir (str, optional): Path to the DBT profiles directory
        """
        self.project_dir = project_dir or os.path.join(os.getcwd(), "models")
        self.profiles_dir = profiles_dir or os.path.join(os.getcwd(), "models", "profiles")
        
        # Ensure directories exist
        os.makedirs(self.project_dir, exist_ok=True)
        os.makedirs(self.profiles_dir, exist_ok=True)
        
        logger.info(f"DBTRunner initialized with project_dir={self.project_dir}, profiles_dir={self.profiles_dir}")
    
    def _run_command(self, command, cwd=None):
        """
        Run a shell command and return the output.
        
        Args:
            command (list): Command to run as a list of strings
            cwd (str, optional): Working directory to run the command in
            
        Returns:
            tuple: (return_code, stdout, stderr)
        """
        logger.info(f"Running command: {' '.join(command)}")
        
        try:
            # Use DBT Core local only
            env = os.environ.copy()
            
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=cwd or self.project_dir,
                env=env
            )
            
            # Get the output
            stdout, stderr = process.communicate()
            return_code = process.returncode
            
            # Log the result
            if return_code == 0:
                logger.info(f"Command completed successfully with return code {return_code}")
            else:
                logger.error(f"Command failed with return code {return_code}")
                logger.error(f"stdout: {stdout}")
                logger.error(f"stderr: {stderr}")
            
            return return_code, stdout, stderr
        
        except Exception as e:
            logger.error(f"Error running command: {str(e)}")
            raise
    
    def init_project(self, project_name):
        """
        Initialize a new DBT project.
        
        Args:
            project_name (str): Name of the DBT project
            
        Returns:
            bool: True if successful, False otherwise
        """
        logger.info(f"Initializing DBT project: {project_name}")
        
        try:
            # Run dbt init command
            return_code, stdout, stderr = self._run_command(
                ["dbt", "init", project_name],
                cwd=os.path.dirname(self.project_dir)
            )
            
            # Check if successful
            if return_code == 0:
                logger.info(f"Successfully initialized DBT project: {project_name}")
                
                # Update project directory
                self.project_dir = os.path.join(os.path.dirname(self.project_dir), project_name)
                
                return True
            else:
                logger.error(f"Failed to initialize DBT project: {stderr}")
                return False
        
        except Exception as e:
            logger.error(f"Error initializing DBT project: {str(e)}")
            raise
    
    def create_profile(self, profile_name, target_name, type="spark", threads=4, schema="public", **kwargs):
        """
        Create a DBT profile for connecting to a database.
        
        Args:
            profile_name (str): Name of the profile
            target_name (str): Name of the target
            type (str): Type of database (spark, postgres, etc.)
            threads (int): Number of threads to use
            schema (str): Default schema to use
            **kwargs: Additional connection parameters
            
        Returns:
            bool: True if successful, False otherwise
        """
        logger.info(f"Creating DBT profile: {profile_name}")
        
        try:
            # Create profile directory if it doesn't exist
            os.makedirs(self.profiles_dir, exist_ok=True)
            
            # Create profile file path
            profile_path = os.path.join(self.profiles_dir, f"{profile_name}.yml")
            
            # Create profile content
            profile = {
                profile_name: {
                    "target": target_name,
                    "outputs": {
                        target_name: {
                            "type": type,
                            "threads": threads,
                            "schema": schema,
                            **kwargs
                        }
                    }
                }
            }
            
            # Write profile to file
            with open(profile_path, "w") as f:
                import yaml
                yaml.dump(profile, f, default_flow_style=False)
            
            logger.info(f"Successfully created DBT profile: {profile_path}")
            return True
        
        except Exception as e:
            logger.error(f"Error creating DBT profile: {str(e)}")
            raise
    
    def run_models(self, models=None, exclude=None, vars=None, full_refresh=False):
        """
        Run DBT models.
        
        Args:
            models (list, optional): List of models to run
            exclude (list, optional): List of models to exclude
            vars (dict, optional): Variables to pass to DBT
            full_refresh (bool): Whether to do a full refresh
            
        Returns:
            bool: True if successful, False otherwise
        """
        logger.info("Running DBT models")
        
        try:
            # Build command
            command = ["dbt", "run", "--profiles-dir", self.profiles_dir]
            
            # Add models if specified
            if models:
                command.extend(["--models", " ".join(models)])
            
            # Add exclude if specified
            if exclude:
                command.extend(["--exclude", " ".join(exclude)])
            
            # Add vars if specified
            if vars:
                command.extend(["--vars", json.dumps(vars)])
            
            # Add full-refresh if specified
            if full_refresh:
                command.append("--full-refresh")
            
            # Run command
            return_code, stdout, stderr = self._run_command(command)
            
            # Check if successful
            if return_code == 0:
                logger.info("Successfully ran DBT models")
                return True
            else:
                logger.error(f"Failed to run DBT models: {stderr}")
                return False
        
        except Exception as e:
            logger.error(f"Error running DBT models: {str(e)}")
            raise
    
    def test_models(self, models=None, exclude=None):
        """
        Test DBT models.
        
        Args:
            models (list, optional): List of models to test
            exclude (list, optional): List of models to exclude
            
        Returns:
            bool: True if successful, False otherwise
        """
        logger.info("Testing DBT models")
        
        try:
            # Build command
            command = ["dbt", "test"]
            
            # Add models if specified
            if models:
                command.extend(["--models", " ".join(models)])
            
            # Add exclude if specified
            if exclude:
                command.extend(["--exclude", " ".join(exclude)])
            
            # Run command
            return_code, stdout, stderr = self._run_command(command)
            
            # Check if successful
            if return_code == 0:
                logger.info("Successfully tested DBT models")
                return True
            else:
                logger.error(f"Failed to test DBT models: {stderr}")
                return False
        
        except Exception as e:
            logger.error(f"Error testing DBT models: {str(e)}")
            raise