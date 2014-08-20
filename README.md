libdig-task
===========

Luigi task for creating a dataset from Library digitisation images


Process
-------

Looks up metadata from XML file (sent by Library team)

Re-sizes images into 800x800 and thumbnail

Creates dataset and saves to datastore


TODO
----

This is very much a placeholder task. The data comes from files supplied by Library & Archives. 

When MAM and new Library Catalogue system is inplace, it should switch over to using that.


There is also quite a lot of overlap with ke2mongo. Need to consolidate the import pipeline tasks.
