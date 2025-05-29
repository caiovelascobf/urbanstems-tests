# Listing all Hevo Data Sources

## Table of Contents

- [Setup Instructions](#setup-instructions)
  - [Prerequisites](#prerequisites)
  - [Environment Variables](#environment-variables)
  - [Build and Run](#build-and-run)

## Setup Instructions

### Prerequisites

Make sure you have the following installed on your local development environment:

* [VSCode](https://code.visualstudio.com/)
  * Install `GitLens — Git supercharged` extension and Jupyter Notebook extensions.
* **.venv - Virtual Environment**
  * Upgrade pip outside and inside of your venv to avoid problems: python.exe -m pip install --upgrade pip 
  * cd your_repo_folder
  * python -m venv .venv                            (This will create a virtual environment for the repo folder)
  * source .venv/Scripts/activate
  * Upgrade pip outside and inside of your venv to avoid problems: python.exe -m pip install --upgrade pip
  * Install everything you need for your project from the `requirements.txt` file:
    * `pip install --no-cache-dir -r requirements.txt`  (This will install things within your virtual environment)

Make sure to inclue a .gitignore file with the following information:

* .venv/         (to ignore the virtual environment stuff)
* *.pyc          (to ignore python bytecode files)
* .env           (to ignore sensitive information, such as database credentials)

### Environment Variables
The .gitignore file, ignores the `.env` file for security reasons. However, since this is just for educational purposes, follow the step below to include it in your project. If you do not include it, the docker will not work.

If you want to check the environment variables from your current folder, do:
* printenv (this will show if the environmental variables were loaded within the Docker container)
* printenv | grep HEVO (this functions as a filter to show only the variables that contain 'HEVO')

### Build and Run

1. **Clone the repository:**

   ```bash
   git clone https://github.com/caiocvelasco/end-to-end-data-science-project.git
   cd end-to-end-data-science-project

2. **Activate you Virtual Environment (.venv)**

* cd your_repo_folder
* source .venv/Scripts/activate                   (This will activate your environment)