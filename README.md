# DAT108U Data Warehouse Project

Welcome to the DAT108U Data Warehouse project repository! This repository contains the code and resources related to the Data Warehouse project for the DAT108U course. In this README, you will find information about the project, its structure, and how to set it up and run it.

## Table of Contents

- [Project Overview](#project-overview)
- [Repository Structure](#repository-structure)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)
- [Contact Information](#contact-information)

## Project Overview

The DAT108U Data Warehouse project is designed to demonstrate the concepts learned during the course. The project involves creating a data warehouse solution to store and manage large volumes of data, enabling efficient querying and analysis. The primary goal of the project is to provide insights and support decision-making based on the data stored in the warehouse.

## Getting Started

### Prerequisites

Before you can run the project, make sure you have the following prerequisites installed:

- [Python](https://www.python.org/) (version 3.6 or higher)
- Other dependencies specified in the project's requirements file (e.g., `requirements.txt`)

### Installation

1. Clone this repository to your local machine using:

    ```bash
    git clone https://github.com/rash-afkh/DAT108U_DWH.git
    ```

2. Navigate to the project directory:

    ```bash
    cd DAT108U_DWH
    ```

3. Install the required dependencies using:
  
    ```bash
      pip install -r requirements.txt
    ```
    
### Usage

  - Configuration: Configure the necessary settings in a `dwh.cfg` file, following the templeate `dwh_template.cfg`.

  - Data Ingestion: Data ingestion scripts (`etl.py`) fetches and loads data into the data warehouse.

  - ETL Process: ETL (Extract, Transform, Load) script (`etl.py`) transforms and loads data from the staging area to the data warehouse tables.

  - Querying and Analysis: SQL queries (`sql_queries.py`) has the list of the queries.


### Contributing

Contributions to this project are welcome! If you find any issues or have improvements to suggest, please open an issue or submit a pull request.
License

This project is licensed under the MIT License.
Contact Information

If you have any questions or need further assistance, you can contact the project owner:

    Name: [Rash Afkhami]
    Email: [rash.afkhami@gmail.com]

Feel free to explore the code, contribute, and use this project as a learning resource. Happy coding!

csharp

Just copy and paste this Markdown content into your README.md file on GitHub to create a well-formatted README.
