# Programming Exercise using PySpark:

A very small company called **KommatiPara** that deals with bitcoin trading has two separate datasets dealing with clients that they want to collate to starting interfacing more with their clients. One dataset contains information about the clients and the other one contains information about their financial details.

The company now needs a dataset containing the emails of the clients from the United Kingdom and the Netherlands and some of their financial details to starting reaching out to them for a new marketing push.


## Features
- CSV reader.
- Remove personal identifiable information from the first dataset(emails).
- Filter clients from the United Kingdom or the Netherlands.
- Remove credit card number from the second dataset.
- Data joined using the id field.
- Rename the columns for the easier readability to the business user.
- Save the output in a **client_data** directory in the root directory of the project.
