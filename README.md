# Listing all Hevo Data Sources

## Table of Contents

- [Setup Instructions](#setup-instructions)
  - [Prerequisites](#prerequisites)
  - [Environment Variables](#environment-variables)
  - [SSH GitHub Setup](#ssh-github-setup)
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

### SSH GitHub Setup

This guide documents how to configure a dedicated SSH key for pushing to GitHub as the user **`caiovelascobf`**, using a clean and secure workflow.

---

1. **Generate a New SSH Key**

```bash
ssh-keygen -t ed25519 -C "your_email_for_caiovelascobf"
```

- When prompted:
  - **Save key as**: `/c/Users/YOUR_USERNAME/.ssh/id_ed25519_caiovelascobf`
  - **Optional passphrase**: enter one or leave it blank

---

2. **Add SSH Key to GitHub**

```bash
cat ~/.ssh/id_ed25519_caiovelascobf.pub
```

- Copy the output
- Go to GitHub → `Settings > SSH and GPG Keys`
- Click **"New SSH key"**
- Name it (e.g., `caiovelascobf_work`)
- Paste and save the key

---

3. **Configure SSH to Use This Key**

Edit (or create) your SSH config file:

```bash
nano ~/.ssh/config
```

Add:

```ssh
Host github-caiovelascobf
  HostName github.com
  User git
  IdentityFile ~/.ssh/id_ed25519_caiovelascobf
  IdentitiesOnly yes
```

This creates a GitHub alias that tells SSH to use this specific key when connecting.

---

4. **Update the Git Remote**

In your project folder:

```bash
git remote set-url origin git@github-caiovelascobf:caiovelascobf/REPO_NAME.git
```

Verify:

```bash
git remote -v
```

Expected:

```
origin  git@github-caiovelascobf:caiovelascobf/REPO_NAME.git (fetch)
origin  git@github-caiovelascobf:caiovelascobf/REPO_NAME.git (push)
```

---

### 5. **Test SSH Authentication**

```bash
ssh -T git@github-caiovelascobf
```

Expected:

```
Hi caiovelascobf! You've successfully authenticated, but GitHub does not provide shell access.
```

---

### 6. **Push to GitHub**

```bash
git add .
git commit -m "Your commit message"
git push origin main
```

You should now be pushing as the `caiovelascobf` user via the correct SSH key.


### Build and Run

1. **Clone the repository:**

   ```bash
   git clone https://github.com/caiocvelasco/end-to-end-data-science-project.git
   cd end-to-end-data-science-project

2. **Activate you Virtual Environment (.venv)**

* cd your_repo_folder
* source .venv/Scripts/activate                   (This will activate your environment)