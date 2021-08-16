## Setting up your environment
1. Run the following command to install `virtualenv` 
    ```
    pip install virtualenv
    ```
2. To create a virtualenv, run the following command from the root of the source directory. 
    ```
    virtualenv venv
    ```
3. Activate the virtualenv, by running:

    Mac/Linux
    ```shell
    source venv/bin/activate
    ```
    Windows
    ```powershell
    venv/scripts/activate
    ```
    
## Installing the dependencies
1. Activate your virtualenv, and run the following command from the root of the source directory
    ```shell
    pip install -r requirements.txt
    ```

## How to run tests
To run the tests, you will need to add the following environment variables. 
1. Create an `.env` file with the following variables
    - `DETA_SDK_TEST_PROJECT_KEY`: Project Key for your deta project
    - `DETA_SDK_TEST_DRIVE_NAME` : Name of your Drive 
    - `DETA_SDK_TEST_DRIVE_HOST` : Host URL (default `"drive.deta.sh"`)
    - `DETA_SDK_TEST_BASE_NAME` : Name of your Base
2. Run
   ```
   python tests.py
   ``` 
   
🎉 Now you are ready to contribute!
   
## How to contribute
1. Make a feature branch
2. Make a draft PR
3. Make your changes to the feature branch
4. Mark draft as ready for review

## How to release
1. Add changes to `CHANGELOG.md`
2. Merge the `master` branch with the `release` branch.
3. After scripts finish, update release and tag with relevant info
