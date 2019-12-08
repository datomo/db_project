# Database Project
## Pain Pill Usage, Crime Rate and Yelp Reviews

This repositiory should serve as a base for all the needed stuff for our database project of the database course
at university Basel.


## Needed Software

* [python 3](https://www.python.org/)

* [mysql connector python](https://www.mysql.com/products/connector/)
```
python -m pip install mysql-connector
```


## Setup

* "local" running mysql instance on default port 3306 

  * with a configured database with the name ```db_project```
  
  (*both options can be configured in the ```database.py file```*)
  
* Data source files are not included in the reporsitory, for more check out **Data Files**
  
  Those need to be placed in the ```data``` folder ( **Careful** naming of the source files can change but needs to match our names so the integration process to run correctly)
  
## Getting Started
* The program can be run by starting ```main.py```, by setting the required flags on top of 
the ```main.py``` file to ```True```, some parts of the setup and integration process can be controlled.
The setup needs to run in sequence 

**Attention:** Running the whole setup process in one go, can take a **extremely** long time depending on your setup 

## Data Files

- https://www.phoenixopendata.com/dataset/crime-data
- https://www.kaggle.com/yelp-dataset/yelp-dataset/download
- https://www.kaggle.com/paultimothymooney/pain-pills-in-the-usa

## Built With

* [MySQL](https://www.mysql.com/) - SQL dialect

* [Python](https://www.python.org/) - Python 3.8


## Authors

* **Isabel Geissmann**  - [isabelge](https://github.com/isabelge)
* **David Lengweiler**  - [datomo](https://github.com/datomo)





