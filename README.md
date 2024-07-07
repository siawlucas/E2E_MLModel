How to Run the Project
Prerequisites
1.	Download and Install:
•	Docker Desktop
•	PostgreSQL
•	Miniconda
Setup Instructions
1.	Open Your Preferred IDE.
2.	Create Miniconda Environment:
•	Command: conda create -n <env_name>
•	Example: conda create -n bfi_test
3.	Activate Conda Environment:
•	Command: conda activate <env_name>
•	Example: conda activate bfi_test
4.	Clone the Repository:
•	Command: git clone https://github.com/siawlucas/E2E_MLModel.git
5.	Install Requirements:
•	Command: pip install -r requirements.txt
6.	Setup Docker to Connect to PostgreSQL:
•	Check and configure docker-compose.yml.
7.	Database Setup:
•	Create a database in PostgreSQL.
•	Create the required tables in the database.
Running the Script
1.	Ensure All Requirements Are Met:
•	Confirm that all dependencies are installed and the database is running.
2.	Run Database Scripts:
•	Command: python <platform>-database.py
•	Command: python <platform>-database-ref.py
3.	Run Data Cleaning Script:
•	Command: python cleansing_<platform>.py
4.	Run Machine Learning Model:
•	Command: python model.py
•	This script performs the machine learning process to generate pricing recommendations based on the data categories.
5.	Run API:
•	Command: python api_2.py
•	This script runs the API to provide access to the processed data and pricing recommendations.
Ensure each step is followed correctly to successfully run the all the script and demonstrate the required functionalities.
