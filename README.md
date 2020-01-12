# Gruppe 4: Final Hand-In

## Pain Pill Usage, Crime Rate and Yelp Reviews

This repositiory should serve as a base for all the needed stuff for our database project of the database course
at university Basel.

## Report

Our findings and information about the whole process can be found in our report here:

- [Report](report/proceedings.pdf)

- http://db-project-group-4.lengweiler.com

## Needed Software

* [python 3](https://www.python.org/)

* [mysql connector python](https://www.mysql.com/products/connector/)
```
python -m pip install mysql-connector
```


## Setup

* "locally" running mysql instance 

  * with a configured schema with the name ```db_multiprocess```
   and following access parameters

        port = 3306
        username = root
        password = db_project2019

  
  (*configured in the ```database.py file```*)
  
* Data source files are not included in the reporsitory, for more check out **Data Files**
  
  Those need to be placed in the ```data``` folder ( **Careful** naming of the source files can change but needs to match our names so the integration process to run correctly)
  
## Getting Started
### Integration
The data can be integrated by running the ```.py```-files in following order:

#### Pain Pill usage in the US
* serializer.py
* splitter.py
* filter.py
* inserter.py
* report_inserter.py

#### Phoenix Crime
* crime_serializer_filter.py
* crime_address.py

#### Yelp
* yelp_filter.py
* yelp_address.py
* yelp_business.py
* yelp_review.py
* yelp_is_located.py
* yelp_add_info_business.py
 

**Attention:** Running the whole setup process in one go, can take a **extremely** long time depending on your setup 

### Analysis
Our analysis results can be viewed here:

http://db-project-group-4.lengweiler.com

or by running the webapplication locally by moving into the **web** folder:
```
    cd web
```
and then executing:
```
    npm install
    npm run serve
```

## Data Files

- https://www.phoenixopendata.com/dataset/crime-data
- https://www.kaggle.com/yelp-dataset/yelp-dataset/download
- https://www.kaggle.com/paultimothymooney/pain-pills-in-the-usa
- https://www.washingtonpost.com/graphics/2019/investigations/dea-pain-pill-database/

## Built With

* [MySQL](https://www.mysql.com/) - SQL dialect

* [Python](https://www.python.org/) - Python 3.8

* [MySQL Connector](https://www.mysql.com/products/connector/) - Python Mysql connector
* [VueJs](https://vuejs.org/) - Progressive JavaScript Framework
* [Tailwind](https://tailwindcss.com/) - CSS Framework


## Authors

* **Isabel Geissmann**  - [isabelge](https://github.com/isabelge)
* **David Lengweiler**  - [datomo](https://github.com/datomo)





