# streaming-04-bonus-ACoffin
> Created by: A. C. Coffin | Date: 24 May 2024 | 
> NW missouri State University | CSIS: 44671-80/81: Data Streaming | Dr. Case

# Overview:
Demonstrating the application of a Round Robin Schedule with RabbitMQ to analyisze MTA Subway Data. This project is a continued exploration into the MTA Data utilized in [streaming-03-bonus-acoffin](https://github.com/accoffin12/streaming-03-rabbitmq). 

# Table of Contents
1. [File List](File_List)
2. [Machine Specs](Machine_specs)
3. [Prerequisites](Prerequisites)
4. [Before you Begin](Before_you_begin)
5. [Data Source](Data_Source)
    * [About the NYC Subway System](About_the_NYC_Subway_System)
6. [Modifications of Data](Modifications_of_Data)
7. [Creating an Enviroment & Installs](Creating_an_Enviroment_&_Installs)

# 1. File List
| File Name | Repo Location | Type |
| ----- | ----- | ----- |
| util_about.py | utils folder | python script |
| util_aboutenv.py | utils folder | python script |
| util_logger.py | utils folder | python script |
| aboutenv.txt | util_outputs folder | python script |
| util_about.txt | util_outputs folder | python script |
| Subway Map.pdf | Maps Folder | PDF |
| SubwayMap.png | Maps Folder | PNG |
| Data_MTA_Subway_Hourly_Ridership.csv | main repo | CSV |

# 2. Machine Specs
This project was created using a Windows OS computer with the following specs. These are not required to run the repository. For further details on this machine go to the "utils folder" and open the "util_output folder" to access "util_about.txt". The "util_about.py" was created by NW Missouri State University and added to the repository to provide technical info.

* Date and Time: 2024-05-24 at 11:23 AM
* Operating System: nt Windows 10
* System Architecture: 64bit
* Number of CPUs: 12
* Machine Type: AMD64
* Python Version: 3.12.3
* Python Build Date and Compiler: main with Apr 15 2024 18:20:11
* Python Implementation: CPython
* Terminal Environment:        VS Code
* Terminal Type:               cmd.exe
* Preferred command:           python

# 3. Prerequisites
1. Git
2. Python 3.7+ (3.11+ preferred)
3. VS Code Editor
4. VS Code Extension: Python (by Microsoft)
5. RabbitMQ Server Installed and Running Locally
6. Anaconda Installed

# 4. Before you Begin
1. Fork this starter repo into your GitHub.
2. Clone your repo down to your machine.
3. View / Command Palette - then Python: Select Interpreter
4. Select your conda environment.

# 5. Data Source
The Metropolitan Transportation Authority(MTA) is responsible for all public transport in New York City and collects data in batches by the hour. This batching creates counts for the number of passengers boarding a subway at a specific station. It also provides data concerning payment, geography, time, date, and location of moving populations based on stations.

MTA Data is readily available from New York State from their Portal.

NYC MTA Data for Subways: https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-Beginning-February-202/wujg-7c2s/about_data

## 5a. About the NYC Subway System

The New York City Subway system has 24 subway lines and 472 stations throughout Manhattan, Brooklyn, Wueens and the Bronx. Statan Island does not have a subway system but a ferry system and an above ground train. The lines are listed in the chart based on their Line Reference. Some Lines do have local express services that share a line but stop at different stations. For the full MTA Subway Map view [Subway Map.pdf](Maps/Subway%20Map.pdf).

| Line Reference | Line Name | Area of NYC |
| ----- | ----- | ----- |
| 1, 2, 3 | Red Line | Runs along the west side of Manhattan |
| 4, 5, 6 | Green Line | East side of Manhattan and parts of the Bronx |
| 7 | Flushing Line | Connects manhattan to Queens |
| A, C | Blue Line | Runs from norther Manhattan through Brooklyn |
| B, D | Orange Line | Connects Manhattan to Brooklyn |
| E, F, M | Purple Line | Serves Queens and Manhattan |
| G | Light Green Line | Connects Brooklyn and Queens |
| J, Z | Brown Line | Runs through Brookelyn and into Queens |
| L | Gray Line | Connects Manhattan and Brooklyn |
| N, Q, R | Yellow Line | serves Manhattan, Brooklyn and Queens |
| S | 42nd Street Shuttle | Short Shuttle line in Manhattan |
| W | White Line | Runs between Manhattan and Queens |
| Z | Jamaica Line | Connects Brooklyn and Queens |

![MTA Subway Map](Maps/SubwayMap.PNG)



# 6. Modifications of Data
The original source contained 12 columns, this was altered to a total of 7 columns. The original column from the data set called "transit_time" has been split into only date and time. Time has also been converted to military time for clarity. There are 56.3 million rows in this set, the same as the original.

The focus of this project is on transit_date, transit_time, station_complex_id, station_complex, borough, and rideship. The columns "payment", "fare", "transfers", "lat", "long" and "geo-reference have been removed. 

# 7. Creating an Enviroment & Installs
Before beginning this project two environments were made, one as a VS Code environment and the other as an Anaconda environment. RabbitMQ requires the Pika Library to function, to ensure that the scripts execute and create an environment in either VS Code or Anaconda.

VS Code Environments allow us to create a virtual environment within the workspace to isolate Python projects with its pre-installed packages and interpreter. For light projects, this is optimal as VS Code environments will not touch other environments or Python installations. However, pre-installed packages can be limited, and the environments will only stay within the selected folder. Meaning that you can't simply call in another environment. The second method of creating an Anaconda environment is different, it's designed for a heavier workload. This method creates a specific reusable environment with specific Python versions and pre-installed packages. However, this method can be heavier due to the additional packages.

While the Anaconda Environment is not necessary for this project it was utilized to ensure that the environments between VS Code and Anaconda were consistent when running the v1 and v2 emitters in VS Code with the v1 and v2 listening scripts running in Anaconda.

## 7a. Creating VS Code Environment
To create a local Python virtual environment to isolate our project's third-party dependencies from other projects. Use the following commands to create an environment, when prompted in VS Code set the .venv to a workspace folder and select yes.
```
python - m venv .venv # Creates a new environment
.venv\Scripts\activate # Activates the new environment
```
Once the environment is created install the following:
```
python -m pip install -r requirements.txt
```
For more information on Pika see the [Pika GitHub](https://github.com/pika/pika)

## 7b. Creating Anaconda Environment
To create an Anaconda environment open an Anaconda Prompt, the first thing that will pop up is the base. Then we are going to locate our folder, to do this type the following:
```
cd \Dcuments\folder_where_repo_is\ 
cd \Documents\ACoffinCSIS44671\streaming-04-bonus-ACoffin # This is where the file is located on my computer
```
Once the folder has been located the line should look like this:
```
(base) C:\Users\Documents\folder_where_repo_is\streaming-04-bonus-ACoffin>
(base) C:\Users\Tower\Documents\ACoffinCSIS44671\streaming-04-bonus-ACoffin> # My File Path
```
To create an environment do the following:
```
conda create -n RabbitEnv # Creates the environment
conda activate RabbitEnv # Activates Environment
This will create the environment, if you want to deactivate it, enter: conda deactivate
```

Once the environment is created execute the following:
```
python --version # Indicates Python Version Installed
conda config --add channels conda-forge # connects to conda forge
conda config --set channel_priority strict # sets priority
install pika # library installation
```
Be sure to do each individually to install Pika in the environment. You have to use the forge to do this with Anaconda.

## 7c. Setup Verification
To verify the setup of your environment run both util_about.py and util_aboutenv.py found in the util's folder or use the following commands in the terminal. These commands are structured for Windows OS if using MacOS or Linux modified to have them function. Also, run the pip list in the terminal to check the Pika installation.
```
python ".\\utils\util_about.py"
python ".\\utils\util_aboutenv.py"
pip list
```
# 8. Method
For this project we will be utilizing a Round Robbin Schedule to pull data from multiple columns. The objective is to send one column to one Consumer and another Column to a different one but have both being emitted by the same Producer. In this case we will be using the first producer to pull only stops related to the Number 7 Flushing Line. This line connects Manhattan to Queens, specifically Long Island City. We want to know how many people are using this line. The second Consumer will be looking for the Hunter's Point Station and capitalizing the letters. Both of these will feed into a csv file called "MTA_7Flushing_Output.csv". 

The following Stations are on the Number 7, Flushing Line:
| station_complex_id | station_complex |
| ----- | ----- |
| 447 | Flushing-Main St (7) |
| 448 | Mets-Willets Point (7) |
| 449 | 111 St (7) |
| 450 | 103 St-Corona Plaza (7) |
| 451 | Junction Blvd (7) |
| 452 | 90 St-Elmhurst Av (7) |
| 453 | 82 St-Jackons Hts (7) |
| 455 | 69 St (7) |
| 456 | 61 St-Woodside (7) |
| 457 | 52 St (7) |
| 458 | 46 St-Bliss St (7) |
| 459 | St-Lowery St (7)|
| 460 | 33 St-Rawson St (7) |
| 461 | Queensboro Plaza (7, N, W) |
| 463 | Hunters Point Av (7) |
| 464 | Vernon Blvd-Jackson Av (7) |
| 471 | 34 St-Hudson Yards (7) |
| 606 | Court Sq (E, G, M, 7) |