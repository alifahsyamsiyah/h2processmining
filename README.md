# H2 Database Engine for Process Mining

An implementation of the [H2 Database Engine](http://www.h2database.com/) that includes a [directly follows relation](https://doi.org/10.1109/TKDE.2004.47), which can be used for process mining.

# Use
Let LOG be a table which includes caseid, activity, and timestamp.
The following query will return directly follows relation in the LOG.

SELECT * FROM DIRECTLYFOLLOWS (SELECT * FROM LOG)
