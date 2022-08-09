# Cancelled Mandates (CM)

## Input

Download Data as csv files:

1. CRM Data:              "crm-accounts-YYYY-MM-DD.csv"
2. Elevate Data           "elevate-accounts-YYYY-MM-DD.csv"
3. Failed Mandates        "failed-mandates-YYYY-MM-DD.csv"
4. Cancelled Mandates     "cancelled-mandates-YYYY-MM-DD.csv"

## Target

Get a CRM account number per failed or cancelled mandate.

## Methods to find a CRM account number

For each row in Failed and Cancelled Mandates "mandate_row", process sequence of methods until a CRM account number is found:

1. Check field "mandate_row"."customers.metadata.accountNumber", if it contains a valid CRM account number: 
   - yes: CRM account number is found, stop process.
   - no: continue with next check

2. Check field "mandate_row"."mandates.id", if we find a match in "elevate"."mandate_reference", if found, use the row's "elevate"."customer_account_number" to find its CRM account by matching "crm"."account_number".
   - yes: CRM account number is found, stop process.
   - no: continue with next check

3. Check field "mandate_row"."customers.id", if we find a match in "crm"."C0 Go Cardless Customer ID", the "crm" account number is found.
   - yes: CRM account number is found, stop process
   - no: continue with next check

4. Check concatenated field "mandate_row"."customers.given_name" & " " & "customer.family_name" against "crm"."C0 Name".
   - yes: CRM account is found, stop process
   - no: continue with next check

## Data to be enriched:

### Assign Team

Extract "crm"."stage name" (e.g. ACTIVE, SOLD, ...) and assign the corresponding team depending on stage name:

case "crm"."stage name":
  when: "SOLD"          : team = 'Pre Installation'
  when: "INSTALL"       : team = 'Pre Installation'
  when: "PROVISIONING"  : team = 'Post Installation'
  when: "INVOICING"     : team = 'Post Installation'
  when: "ACTIVE"        : team = 'Post Installation'
  when: "INACTIVE"      : team = 'No action - inactive'
  when others           : team = 'Pre Installation'

If "mandate_row"."details.description" contains the words: "(.*)at your request(.*)" assign "team" = "No action - at our request".

### Additional Fields to add:

- "crm"."account_number"
- "crm"."stage_name"
- "mandate_row".(concatenate("customers.given_name" + " " + "customers.family_name"))
- "crm"."C 0 Name"
- "crm"."premise_address"
- "crm"."C 0 Email"
- "crm"."C 0 ID"

### Processing Logic

if we have already an entry for that event "failed/cancelled" mandates from previous days, skip the record. If not...

Create file 1: "mandates-to-process-by-pre-installation-team-YYYY-MM-DD.csv", if "team" = "Pre Installation"

Create file 2: "mandates-to-process-by-post-installation-team-YYYY-MM-DD.csv", if "team" = "Post Installation"

## Download Executable

Click on cm and download the executable
Create a folder to host this executable (and all other files later)

## Solve Security Topic "Unverified Developer"

You can bypass the block in your Security & Privacy settings manually:

1. Open the Apple menu, and click System Preferences.
2. Click Security & Privacy.
3. Click the General tab.
4. Click the lock in the lower right corner of the window.
5. Enter your username and password when prompted, and click Unlock.
6. Click the App Store and Identified Developers radial button.
7. Look for “fp was blocked from opening because it is not from an identified developer” and click Open Anyway. (In older versions of macOS, you could click Anywhere and then click Allow From Anywhere.)
8. Try rerunning the app.

## Usage

Just run...

```bash
./cm
```

This will use the default values:

```
Parameter:      Default value:
- db            = cancelled-mandates-database.sqlite3
- elevate       = elevate-accounts-YYYY-MM-DD.csv                                   with today's accounts from Elevate System
- crm           = crm-accounts-YYYY-MM-DD.csv                                       with today's/week's accounts from CRM System
- cancelled     = cancelled-mandates-YYYY-MM-DD.csv                                 with today's date: YYYY=year, MM=month, DD=day)
- failed        = failed-mandates-YYYY-MM-DD.csv                                    with today's date: YYYY=year, MM=month, DD=day)
- toPre         = mandates-to-process-by-pre-installation-team-YYYY-MM-DD.csv       with today's date: YYYY=year, MM=month, DD=day)
- toPost        = mandates-to-process-by-post-installation-team-YYYY-MM-DD.csv      with today's date: YYYY=year, MM=month, DD=day)
- toCheck       = mandates-to-check-YYYY-MM-DD.csv                                  with today's date: YYYY=year, MM=month, DD=day)
```

If you want to have more control, use the parameters and provide a value for a parameter such as the following example:

```bash
./cm -db cancelled-mandates-database.sqlite3 -from cancelled-mandates-2022-05-28.csv -toPre mandates-to-process-by-pre-installation-team-2022-05-28.csv -toPost mandates-to-process-by-post-installation-team-2022-05-28.csv -toCheck mandates-to-check-2022-05-28.csv
```

## What it does

![Process Flow](/documentation/cm-process.png)

The above program does the following:

- it reads accounts records from a `-accounts` file-name.csv, which you download from your Elevate system
- it reads crm accounts records from a `-crm` file-name.csv, which you download from your CRM system
- it reads cancelled mandates records from a `-from` file-name.csv, which you download from your payment provider
- if this parameter isn't provided, it opens the csv file: cancelled-mandates-YYYY-MM-DD.csv
- it writes those records to a local `-db` dbname.sqlite3 database, and...
  - if the cancelled-mandate event id is already in the database, it skips that record, as it is already in the database, or...
  - if the cancelled-mandate event id is not in the database, it inserts the record into the database.
- after inserting all provided new records...

### Pre Processing Team

- it checks the database for event ids 
- if found, create a record in the table preProcessing or postProcessing with
  - event_id as unique primary key
  - timestamp with today's date as secondary index
  - customers_id, customers_given_name, customers_family_name
- create a csv-file mandates-to-process-by-pre-installation-team-YYYY-MM-DD.csv containing all mandates to be processed
- enriching the the files with the account-numbers for the Elevate system for easier processing

### Post Processing Team

- it checks the database for payments_id, which had `-count-suspend` number or more of payment requests (default: 4)
- if found, create a record in the table paymentsSuspended
  - payments_id as unique primary key
  - timestamp with today's date secondary index
  - payment_requests_count
  - customers_id, customers_given_name, customers_family_name, customers_metadata_leadID
- if paymentsId already in the table paymentsSuspended, check if the the payment_requests_count has increased,
  - if it is increased, update the record with the new count and update the timestamp to today's date
  - if it is the same count, then skip the update.
- create a csv-file customers-to-suspend-YYYY-MM-DD.csv containing all customer payments which are to suspend by using today's timestamp
- the found payments_id with more than the allowed number of payment requests are exported in a new `-to` customers-to-suspend-YYYY-MM-DD.csv file
- if the customers-to-suspend-YYYY-MM-DD.csv file already exists, it will be overwritten with the new content

![Process Flow](/documentation/cm-export.png)

## How to import mandates-to-process-by-pre-installation-team-YYYY-MM-DD.csv files into Excel

1. Open a new empty Excel file
2. In Excel choose menu "Data"
3. Click on the Dropdown icon of the first menu item: Something like "Import Data from..." (Don't know the exact translation, as I'm using a German version)
4. In the Popup-Menu choose: "From Text (Legacy)"
5. In the File-open-Popup window choose the path and file of the mandates-to-process-by-pre-installation-team-YYYY-MM-DD.csv file
6. Click on "Import Data" button
7. In the Text-Conversion-Assistant Step 1 of 3, choose radio-button "With delimiters - such as Commas,..."
8. Click "Next"
9. In the Text-Conversion-Assistant Step 2 of 3, choose only the checkbox: [x] "Comma"
10. Click "Next"
11. Click "Finish or Finalize"
12. If there is another popup window asking you for the cell location, just accept the suggestion with button "OK"

If it is a lot of data records, it takes a moment to get all records into the view.

## How to change the program

This is a Go program compiled in version 1.18. If you need to adjust the program to your requirements you might copy and change it.

### Setup Go

You need the Google Go language compiler installed on your machine in order to adjust and build an executable such as the above cm.exe

The installation procedure depends on your operating system and chipset of your computer. [Go Installation](https://go.dev/doc/install)

### Setup Git Version Management (optional)

[Git Installation](https://git-scm.com/downloads)

### Download the Source Code

Whether by using

```bash
git clone https://github.com/tobkle/cancelled-mandates
```

or by downloading the zip archive from [this Github Archive](https://github.com/tobkle/cancelled-mandates) and unzip on your computer.

### Adjust the code

In any Source Code editor. You might use for example the free [Microsoft Visual Studio Code](https://code.visualstudio.com/download)
After downloading, installing and opening, you might add the Go Language package to have the Code Syntax Checker for Golang.

### Compile the code

You can compile the code for different platforms:

For OS X open the terminal program, go into the directory of the code and execute:

```bash
go build
```

You can cross compile for Windows platform:

```bash
env GOOS=windows GOARCH=386 go build
```

You can cross compile for Linux platform:

```bash
env GOOS=linux GOARCH=arm go build
```

You just have to choose a valid combination of the environment variables:

- `GOOS` = Operating System (OS)
- `GOARCH` = Chipset Architecture

Choose a valid combination from the following table:

| $GOOS     | $GOARCH  |
| --------- | -------- |
| aix       | ppc64    |
| android   | 386      |
| android   | amd64    |
| android   | arm      |
| android   | arm64    |
| darwin    | amd64    |
| darwin    | arm64    |
| dragonfly | amd64    |
| freebsd   | 386      |
| freebsd   | amd64    |
| freebsd   | arm      |
| illumos   | amd64    |
| ios       | arm64    |
| js        | wasm     |
| linux     | 386      |
| linux     | amd64    |
| linux     | arm      |
| linux     | arm64    |
| linux     | ppc64    |
| linux     | ppc64le  |
| linux     | mips     |
| linux     | mipsle   |
| linux     | mips64   |
| linux     | mips64le |
| linux     | riscv64  |
| linux     | s390x    |
| netbsd    | 386      |
| netbsd    | amd64    |
| netbsd    | arm      |
| openbsd   | 386      |
| openbsd   | amd64    |
| openbsd   | arm      |
| openbsd   | arm64    |
| plan9     | 386      |
| plan9     | amd64    |
| plan9     | arm      |
| solaris   | amd64    |
| windows   | 386      |
| windows   | amd64    |
| windows   | arm      |
| windows   | arm64    |

[Source](https://go.dev/doc/install/source#environment)

## Windows 64 Compilation

```
brew install mingw-w64
env GOOS="windows" GOARCH="amd64" CGO_ENABLED="1" CC="x86_64-w64-mingw32-gcc" go build
```

SQLITE requires CGO_ENABLED=1, default is 0.
CGO requires windows.h library, this is included in mingw

## Permission Denied on Mac OSX

**Program is from an unverified programmer/vendor**

Solution:
Go to OS X: System Preferences –> Data Privacy –> Unlock –> Allow fp

**Permission Denied, when you try to run**

Solution:

- start Terminal App
- change directory where fp is located by command

```
cd <path where cm is located >
chmod 777 fp
```
