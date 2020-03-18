KyT -- Know your transactions
=====================================
KyT is an extenstion to CAST AIP Platform that illustrates risks carried by transactions of already analysed applications.


KyT will render riskiest paths of transactions in PNG or SVG format as well as HTML/JS view, and also will generate CAST Enlighten view.

Samples of KyT outputs below, using CAST analysis made on OWASP Webgoat application, itself sample application used to test vulnerabilities commonly found in Java:

[<img src="./.github/kyt-sample-webgoat-1.png" width="24%">](./.github/kyt-sample-webgoat-1.png "Longest riskiest path, PNG format")
[<img src="./.github/kyt-sample-webgoat-2.svg" width="24%">](./.github/kyt-sample-webgoat-2.svg "All riskiest path, SVG format")
[<img src="./.github/kyt-sample-webgoat-3.png" width="24%">](./.github/kyt-sample-webgoat-3.png "All riskiest path, HTML/JS format")
[<img src="./.github/kyt-sample-webgoat-4.png" width="24%">](./.github/kyt-sample-webgoat-4.png "All riskiest path, CAST Enlighten")

Documentation for this CAST Extension can be found in the Wiki section of this site (https://github.com/CAST-Extend/com.castsoftware.uc.kyt/wiki).


### Description
KyT is based upon analysis results performed by CAST and its system level capabilities.

Given a transaction, KyT will explore all the paths from the entry point and depending on the algorithm used, it will select one or more 'paths of interest':
  - some algorithms will select only one path: one of the longest path from the transaction entry point to a end point of the transaction, going throught the highest number of critical violations

    Variant algorithms differ in the way they treat end points: some restrict end points to be table; and in the way of handling cycles insides path
  - some algortihms will select all riskiest paths: paths from transaction entry point to transaction's end points and going throught critical violations
  - another algorithm will select all riskiest paths from transaction's entry point to critical violations, stopping exploration at the last critical violation met

Paths are then graphically illustrated: in a PNG, SVG or HTML/JS view or inserted into CAST Enligthen tool.
  
