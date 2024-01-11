# Testing

- Unit tests were used in this project to run code alongside, in order to check for correct outcomes, and any validation issues.
- The unit tests were written along with the code, in order to implement a TDD-driven process.

## Unit Tests
### The Unit Tests are Broken down into 2 test classes:
1. Tests for the Extract Class
2. Tests for the Transform Class

In both classes there are 6 functions, one for each data file type being extracted and transformed. 

**These Consist of:**
- Business files
- Engineering files
- Data files
- Applicants
- JSON files
- TXT files

---
### Extract Tests
The extract tests consist of:
- 3 tests where a single row of data is passed in, and the returned DataFrame is checked to see it is as expected.
- 3 tests where the actual data is read in, and the resulting column names are checked to ensure they are correct.

### Transform Tests  
- The transform functions include the addition and removal of columns.
- The tests for the transform methods runs them, and checks that columns have been added or dropped.
   - For JSON we also check the number of rows.
 
### Load Tests 
