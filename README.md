# Springboard Capstone GTFS Data Engineering


# Goal
The goal is to create a data-warehouse of transit delay events and their related attribute information for data analysis.

# Description
The project uses GTFS feed data and twitter data as its source. The source data is extracted from the source APIs, converted into either json or txt files, and stored in a data lake. Next, the data is transformed into individual delay occurrences, or attributes related to delay occurrences. Finally, it is loaded into a data-warehouse for querying and analysis.

# GTFS
GTFS or general transit feed specification is a well documented data specification created by Google and partner transit agencies. GTFS allows transit agencies to publish their transit data in a documented format that companies and developers can integrate into their applications. GTFS comes in static files and real time data feeds. The static files provide information on stops (stations), routes, trips, etc. The real time feeds provide information on train locations and expected arrival times. This project uses a combination of both types of files to create the delay occurrence events. 
