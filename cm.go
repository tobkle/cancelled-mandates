/********************************************************************************************************************
 * name: cm -- Processing Cancelled and Failed Mandates
 * description: read csv files, insert into sqlite3, export data to csv for different mandate stages
 * author: Tobias Klemmer <tobias@klemmer.info>
 * date:    2022-08-09
 * changed: 2022-08-09
 * version: 1
 * state: prototype
 ********************************************************************************************************************/
package main

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"flag"
	"io"
	"log"
	"os"
	"fmt"
//	"strconv"
	"strings"
	"time"
	"path/filepath"
	_ "github.com/mattn/go-sqlite3"
)

type DB struct {
    *sql.DB
}

type CMD struct {
	*sql.Stmt
}

func getCurrentPath() string {
	ex, err := os.Executable()
    if err != nil {
        panic(err)
    }
    exPath := filepath.Dir(ex)
    fmt.Println(exPath)
	return exPath
}


// Create or Open Sqlite3 database with name of provided parameter 
func createDatabase(dbName string) (*DB) {
	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatalf("Cannot connect to database: %s %s", dbName, err)
	}
	return &DB{db}
}

// Prepare an SQL statement for the database
func prepareSQL(name string, statement string, db *DB) (*CMD) {
	command, err := db.Prepare(statement)
	if err != nil {
		log.Fatalf("SQL Statement prepare failed: %s %s", name, err)
	}
	return &CMD{command}
}

// Execute an SQL statement on the database
func executeSQL(name string, command *CMD) {
	_, err := command.Exec()
	if err != nil {
		log.Fatalf("SQL Statement execution failed: %s %s", name, err)
	}
}

// Prepare and execute an SQL statement on the database
func prepareAndExecuteSQL(name string, statement string, db *DB) {
	command := prepareSQL(name, statement, db)
	executeSQL(name, command)
}

// Create or Open Accounts table in Database
func createTableElevateAccounts(db *DB) {
	SQLCreateAccountsDB := `
		CREATE TABLE IF NOT EXISTS elevateAccounts (
			elevate_account_number    text primary key,
			elevate_mandate_reference text,
			elevate_customer_name     text 
	)`
	prepareAndExecuteSQL("create table elevateAccounts", SQLCreateAccountsDB, db)
}

// Create or Open CRM table in Database
func createTableCRMAccounts(db *DB) {
	SQLCreateCRMAccountsDB := `
	  CREATE TABLE IF NOT EXISTS crmAccounts (
		crm_id                text primary key,
		crm_account_number    text,
		crm_name              text,
		crm_email             text,
		crm_premise_address   text,
		crm_stage_name        text,
		crm_gocardless_id     text,
		crm_zen_user_id       text
	)`
	prepareAndExecuteSQL("create table crmAccounts", SQLCreateCRMAccountsDB, db)
}

// Create or Open mandateEvents table in Database
func createTableMandateEvents(db *DB) {
	SQLMandateEvents := `
	  CREATE TABLE IF NOT EXISTS mandateEvents (
		id 										text primary key,	
		created_at 								text,	
		resource_type 							text,	
		action 									text,	
		details_origin 							text,	
		details_cause 							text,	
		details_description	        			text,	
		details_scheme 							text,	
		details_reason_code 					text,	
		links_previous_customer_bank_account	text,	
		links_new_customer_bank_account 		text,	
		links_parent_event 						text,	
		links_mandate 							text,	
		mandates_id 							text,	
		mandates_created_at 					text,	
		mandates_reference 						text,	
		mandates_status 						text,	
		mandates_scheme 						text,	
		mandates_next_possible_charge_date   	text,	
		mandates_payments_require_approval   	text,	
		mandates_links_customer_bank_account 	text,	
		mandates_links_creditor 				text,	
		customers_id							text,
		customers_given_name 					text,	
		customers_family_name 					text,	
		customers_company_name 					text,	
		customers_metadata_leadID 				text,	
		customers_metadata_link 				text,	
		customers_metadata_xero 				text,	
		mandates_metadata_xero 					text,
		imported_at                             text,
		customers_name                          text
	)`
	prepareAndExecuteSQL("create table mandateEvents", SQLMandateEvents, db)
}

// Create Index idx_mandate_events_imported_at
func createIndexMandateEventsTimestamp(db *DB) {
	SQLCreateDBIndexOnMandateEventsTimestamp := `
       CREATE INDEX IF NOT EXISTS idx_mandate_events_imported_at 
	   ON mandateEvents(imported_at)
	`
	prepareAndExecuteSQL("create index idx_mandate_events_imported_at", SQLCreateDBIndexOnMandateEventsTimestamp, db)
}

// Create Index idx_crm_accounts_crm_account_number
func createIndexCRMAccountsAccountNumber(db *DB) {
	SQLCreateIndex := `
       CREATE INDEX IF NOT EXISTS idx_crm_accounts_crm_account_number 
	   ON crmAccounts(crm_account_number)
	`
	prepareAndExecuteSQL("create index idx_crm_accounts_crm_account_number", SQLCreateIndex, db)
}

// Create Index idx_crm_accounts_crm_name
func createIndexCRMAccountsName(db *DB) {
	SQLCreateIndex := `
       CREATE INDEX IF NOT EXISTS idx_crm_accounts_crm_name
	   ON crmAccounts(crm_name)
	`
	prepareAndExecuteSQL("create index idx_crm_accounts_crm_name", SQLCreateIndex, db)
}

// Create Index idx_crm_accounts_crm_gocardless_id
func createIndexCRMAccountsGoCardlessId(db *DB) {
	SQLCreateIndex := `
       CREATE INDEX IF NOT EXISTS idx_crm_accounts_crm_gocardless_id
	   ON crmAccounts(crm_gocardless_id)
	`
	prepareAndExecuteSQL("create index idx_crm_accounts_crm_gocardless_id", SQLCreateIndex, db)
}

// Create Index idx_elevate_accounts_elevate_mandate_reference
func createIndexElevateAccountsMandateReference(db *DB) {
	SQLCreateIndex := `
       CREATE INDEX IF NOT EXISTS idx_elevate_accounts_elevate_mandate_reference
	   ON elevateAccounts(elevate_mandate_reference)
	`
	prepareAndExecuteSQL("create index idx_elevate_accounts_elevate_mandate_reference", SQLCreateIndex, db)
}

// Open CSV File for Accounts
func importElevateAccounts(db *DB, csvFileName string) {
	fileData, err := os.Open(csvFileName)
	if err != nil {
		fmt.Println("Skipping Elevate Accounts file, as there is no current %s file provided....", csvFileName)
	} else {
		// Read the header row
		recordData := csv.NewReader(fileData)
		_, err = recordData.Read()
		if err != nil {
			log.Fatalf("Missing header row(?): %s", err)
		}

		// prepare insert record for Accounts
		SQLInsertAccountsDB := `
			INSERT INTO elevateAccounts(
				elevate_account_number,
				elevate_mandate_reference,
				elevate_customer_name       
			) values(?, ?, ?)
		`
		SQLcommand := prepareSQL("insert into elevateAccounts", SQLInsertAccountsDB, db)

		// Loop over the records
		for {
			// get next record in csv file
			record, err := recordData.Read()

			// End of File reached
			if errors.Is(err, io.EOF) {
				break
			}

			//  Map the fields of a csv record to variables	
			customer_account_number			 := record[0]
			// Customer_ID	                  	 := record[1]
			customer_name					 := record[2]
			// Site_ID							 := record[3]
			// site_reference					 := record[4]
			// product_category_name			 := record[5]
			// product_type				    	 := record[6]
			// service_id						 := record[7]
			// product_reference				 := record[8]
			// supplier_name					 := record[9]
			// override						     := record[10]
			// start_date						 := record[11]
			// end_date						     := record[12]
			// rental_product_name				 := record[13]
			// cap_price_in_pence				 := record[14]
			// provisioning_status				 := record[15]
			// billable						     := record[16]
			// in_flight_order					 := record[17]
			// force_billing					 := record[18]
			// invoice_frequency				 := record[19]
			// bill_initial_charges_immediately  := record[20]
			// contractName					     := record[21]
			// contractStartDate				 := record[22]
			// contractEndDate					 := record[23]
			// EtcFixed						     := record[24]
			// EtcPercentage					 := record[25]
			// contract_expires_in_months		 := record[26]
			// customerContractDueRenewal		 := record[27]
			// customerContractAutoRollOver	     := record[28]
			// contractProfileName				 := record[29]
			mandate_reference				 := record[30]
			// site_address_line1                := record[31]
			// site_address_line2                := record[32]
			// town                              := record[33]
			// county                            := record[34]
			// post_code                         := record[35]
			// country                           := record[36]

			_, err = SQLcommand.Exec(
								customer_account_number   ,
								mandate_reference         , 
								customer_name             )
			if err != nil {
				if strings.Contains(fmt.Sprint(err), "UNIQUE constraint failed") {
					// fmt.Println("SUCCESS: Skipped existing record with id:", customer_account_number)
				} else {
					fmt.Println("ERROR:   Insert into table elevateAccounts failed for id =", customer_account_number, err)
				}
			} else {
					fmt.Println("SUCCESS: Insert into table elevateAccounts with id:", customer_account_number)
			}
		}
		fmt.Println("***********************************************************")
		fmt.Println("PROCESSING ELEVATE ACCOUNTS --   ended")
		fmt.Println("***********************************************************")
		fmt.Println(" ")
	}
}

// Open CSV File for CRM Accounts
func importCRMAccounts(db *DB, csvFileName string) {
	fileData, err := os.Open(csvFileName)
	if err != nil {
		fmt.Println("Skipping CRM Accounts file, as there is no current %s file provided....", csvFileName)
	} else {
		// process only if the CRM Accounts file exists
		// Read the header row
		recordData := csv.NewReader(fileData)
		// recordData.Comma = ';'
		_, err = recordData.Read()
		if err != nil {
			log.Fatalf("Missing header row(?): %s", err)
		}

		// prepare insert record for Accounts
		SQLInsertCRMAccountsDB := `
		INSERT INTO crmAccounts(
			crm_id,
			crm_account_number,
			crm_name,
			crm_email,
			crm_premise_address,
			crm_stage_name,
			crm_gocardless_id,
			crm_zen_user_id
		) values(?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(crm_id) 
		DO UPDATE SET 
		    crm_name=excluded.crm_name,
		    crm_email=excluded.crm_email,
		    crm_premise_address=excluded.crm_premise_address,
		    crm_stage_name=excluded.crm_stage_name,
		    crm_gocardless_id=excluded.crm_gocardless_id,
		    crm_zen_user_id=excluded.crm_zen_user_id
		`
		commandSQL := prepareSQL("insert into crmAccounts", SQLInsertCRMAccountsDB, db)

		// Loop over the records
		for {
			// get next record in csv file
			record, err := recordData.Read()

			// End of File reached
			if errors.Is(err, io.EOF) {
				break
			}

			//  Map the fields of a csv record to variables	
			crm_account_number			  := record[0]
			// C0UserID	                  := record[1]
			crm_premise_address           := record[2]
			// Premise_Type	              := record[3]
			crm_stage_name                := record[4]
			// Status	                  := record[5]
			crm_name	                  := record[6]
			crm_email	                  := record[7]
			crm_gocardless_id             := record[8]	
			crm_id	                      := record[9]
			crm_zen_user_id               := record[10]
			// Count                      := record[11]

			_, err = commandSQL.Exec(
						crm_id,
						crm_account_number,
						crm_name,
						crm_email,
						crm_premise_address,
						crm_stage_name,
					    crm_gocardless_id,
						crm_zen_user_id       )
			if err != nil {
				if strings.Contains(fmt.Sprint(err), "UNIQUE constraint failed") {
					// fmt.Println("SUCCESS: Skipped existing record with id:", customer_account_number)
				} else {
					fmt.Println("ERROR:   Insert into table crmAccounts failed for id =", crm_account_number, crm_id, err)
				}
			} else {
					fmt.Println("SUCCESS: Insert into table crmAccounts with id:", crm_account_number, crm_id)
			}
		}
	}
	fmt.Println("***********************************************************")
	fmt.Println("PROCESSING ELEVATE CRM ACCOUNTS --   ended")
	fmt.Println("***********************************************************")
	fmt.Println(" ")
}

// import mandate events data from specifice file
func importMandateEvents (db *DB, csvFileName string) {
	var timestamp = time.Now().Format("2006-01-02")
	fileData, err := os.Open(csvFileName)
	if err != nil {
		fmt.Println("Skipping Mandate Events file, as there is no current %s file provided....", csvFileName)
	} else {
		// Read the header row
		recordData := csv.NewReader(fileData)
		_, err = recordData.Read()
		if err != nil {
			log.Fatalf("Missing header row(?): %s %s", csvFileName, err)
		}

		// prepare insert record for mandateEvents
		SQLInsertMandateEventsDB := `
		INSERT INTO mandateEvents(
			id 										,	
			created_at 								,	
			resource_type 							,	
			action 									,	
			details_origin 							,	
			details_cause 							,	
			details_description	        			,	
			details_scheme 							,	
			details_reason_code 					,	
			links_previous_customer_bank_account	,	
			links_new_customer_bank_account 		,	
			links_parent_event 						,	
			links_mandate 							,	
			mandates_id 							,	
			mandates_created_at 					,	
			mandates_reference 						,	
			mandates_status 						,	
			mandates_scheme 						,	
			mandates_next_possible_charge_date   	,	
			mandates_payments_require_approval   	,	
			mandates_links_customer_bank_account 	,	
			mandates_links_creditor 				,	
			customers_id							,
			customers_given_name 					,	
			customers_family_name 					,	
			customers_company_name 					,	
			customers_metadata_leadID 				,	
			customers_metadata_link 				,	
			customers_metadata_xero 				,	
			mandates_metadata_xero 					,
			imported_at                             ,
			customers_name
		) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`
		commandSQL := prepareSQL("insert into mandateEvents", SQLInsertMandateEventsDB, db)

		// Loop over the records
		for {
			// get next record in csv file
			record, err := recordData.Read()

			// End of File reached
			if errors.Is(err, io.EOF) {
				break
			}

			//  Map the fields of a csv record to variables	
			id	 									:= record[0]
			created_at	 							:= record[1]
			resource_type	 						:= record[2]
			action	 								:= record[3]
			details_origin	 						:= record[4]
			details_cause	 						:= record[5]
			details_description	 					:= record[6]
			details_scheme	 						:= record[7]
			details_reason_code	 					:= record[8]
			links_previous_customer_bank_account	:= record[9]
			links_new_customer_bank_account	 		:= record[10]
			links_parent_event	 					:= record[11]
			links_mandate	 						:= record[12]
			mandates_id	 							:= record[13]
			mandates_created_at	 					:= record[14]
			mandates_reference	 					:= record[15]
			mandates_status	 						:= record[16]
			mandates_scheme	 						:= record[17]
			mandates_next_possible_charge_date	  	:= record[18]
			mandates_payments_require_approval	  	:= record[19]
			mandates_links_customer_bank_account  	:= record[20]
			mandates_links_creditor	             	:= record[21]
			customers_id	 				      	:= record[22]
			customers_given_name	 			  	:= record[23]
			customers_family_name	 			  	:= record[24]
			customers_company_name	 			  	:= record[25]
			customers_metadata_leadID	 		  	:= record[26]
			customers_metadata_link	 			  	:= record[27]
			customers_metadata_xero	 			  	:= "" //record[28]
			mandates_metadata_xero 				  	:= "" //record[29]
			imported_at                             := timestamp
			customers_name                          := customers_given_name + " " + customers_family_name

			_, err = commandSQL.Exec(
					id 										,	
					created_at 								,	
					resource_type 							,	
					action 									,	
					details_origin 							,	
					details_cause 							,	
					details_description	        			,	
					details_scheme 							,	
					details_reason_code 					,	
					links_previous_customer_bank_account	,	
					links_new_customer_bank_account 		,	
					links_parent_event 						,	
					links_mandate 							,	
					mandates_id 							,	
					mandates_created_at 					,	
					mandates_reference 						,	
					mandates_status 						,	
					mandates_scheme 						,	
					mandates_next_possible_charge_date   	,	
					mandates_payments_require_approval   	,	
					mandates_links_customer_bank_account 	,	
					mandates_links_creditor 				,	
					customers_id							,
					customers_given_name 					,	
					customers_family_name 					,	
					customers_company_name 					,	
					customers_metadata_leadID 				,	
					customers_metadata_link 				,	
					customers_metadata_xero 				,	
					mandates_metadata_xero                  ,
				    imported_at                             ,
				    customers_name                           )

			if err != nil {
				if strings.Contains(fmt.Sprint(err), "UNIQUE constraint failed: mandateEvents.id") {
					fmt.Println("SUCCESS: Skipped existing record mandateEvents with id:", id)
				} else {
					fmt.Println("ERROR:   Insert into table mandateEvents failed for id =", id, err)
				}
			} else {
					fmt.Println("SUCCESS: Insert into table mandateEvents with id:", id)
			}	

		} // for loop
	} // if data
	fmt.Println("***********************************************************")
	fmt.Println("PROCESSING CANCELLED OR FAILED MANDATES          --   ended")
	fmt.Println("***********************************************************")
	fmt.Println(" ")
} // func

// process mandate events for today's records
func processMandateEvents(db *DB, csvPreTeamTo string, csvPostTeamTo string, csvOtherTeamTo string) {
	var timestamp = time.Now().Format("2006-01-02")
	tx, err := db.Begin()

	headerText := "id,created_at,resource_type,action,details_origin,details_cause,details_description,details_scheme,details_reason_code,links_previous_customer_bank_account,links_new_customer_bank_account,links_parent_event,links_mandate,mandates_id,mandates_created_at,mandates_reference,mandates_status,mandates_scheme,mandates_next_possible_charge_date,mandates_payments_require_approval,mandates_links_customer_bank_account,mandates_links_creditor,customers_id,customers_given_name,customers_family_name,customers_company_name,customers_metadata_leadID,customers_metadata_link,customers_metadata_xero,mandates_metadata_xero,imported_at,customers_name,crm_account_number,crm_id,crm_name,crm_email,crm_premise_address,crm_stage_name,crm_customer_name,crm_gocardless_id,target_team,crm_zen_user_id\n"

	SQLTodaysMandateEvents := fmt.Sprintf(`
		SELECT DISTINCT
			id 										,	
			created_at 								,	
			resource_type 							,	
			action 									,	
			details_origin 							,	
			details_cause 							,	
			details_description	        			,	
			details_scheme 							,	
			details_reason_code 					,	
			links_previous_customer_bank_account	,	
			links_new_customer_bank_account 		,	
			links_parent_event 						,	
			links_mandate 							,	
			mandates_id 							,	
			mandates_created_at 					,	
			mandates_reference 						,	
			mandates_status 						,	
			mandates_scheme 						,	
			mandates_next_possible_charge_date   	,	
			mandates_payments_require_approval   	,	
			mandates_links_customer_bank_account 	,	
			mandates_links_creditor 				,	
			customers_id							,
			customers_given_name 					,	
			customers_family_name 					,	
			customers_company_name 					,	
			customers_metadata_leadID 				,	
			customers_metadata_link 				,	
			customers_metadata_xero 				,	
			mandates_metadata_xero 					,
			imported_at                             ,
			customers_name
		FROM mandateEvents
		WHERE imported_at = "%s"
	`, timestamp)

	// prepare file "mandates-to-process-by-pre-installation-team-YYYY-MM-DD.csv"
	targetFilePreTeam, err := os.OpenFile(csvPreTeamTo, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	defer targetFilePreTeam.Close()
	if _, err = targetFilePreTeam.WriteString(headerText); err != nil {
		panic(err)
	}

	// prepare file "mandates-to-process-by-post-installation-team-YYYY-MM-DD.csv"
	targetFilePostTeam, err := os.OpenFile(csvPostTeamTo, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	defer targetFilePostTeam.Close()
	if _, err = targetFilePostTeam.WriteString(headerText); err != nil {
		panic(err)
	}

	// prepare file "mandates-to-process-by-post-installation-team-YYYY-MM-DD.csv"
	targetFileOthers, err := os.OpenFile(csvOtherTeamTo, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	defer targetFileOthers.Close()
	if _, err = targetFileOthers.WriteString(headerText); err != nil {
		panic(err)
	}

	row, err := tx.Query(SQLTodaysMandateEvents)
	if err != nil {
		log.Fatal(err)
	}
	defer row.Close()

	for row.Next() {
			var id 										string 	 
			var created_at 								string 	 
			var resource_type 							string 	 
			var action 									string 	 
			var details_origin 							string 	 
			var details_cause 							string 	 
			var details_description	        			string 	 
			var details_scheme 							string 	 
			var details_reason_code 					string 	 
			var links_previous_customer_bank_account	string 	 
			var links_new_customer_bank_account 		string 	 
			var links_parent_event 						string 	 
			var links_mandate 							string 	 
			var mandates_id 							string 	 
			var mandates_created_at 					string 	 
			var mandates_reference 						string 	 
			var mandates_status 						string 	 
			var mandates_scheme 						string 	 
			var mandates_next_possible_charge_date   	string 	 
			var mandates_payments_require_approval   	string 	 
			var mandates_links_customer_bank_account 	string 	 
			var mandates_links_creditor 				string 	 
			var customers_id							string  
			var customers_given_name 					string 	 
			var customers_family_name 					string 	 
			var customers_company_name 					string 	 
			var customers_metadata_leadID 				string 	 
			var customers_metadata_link 				string 	 
			var customers_metadata_xero 				string 	 
			var mandates_metadata_xero 					string  
			var imported_at                             string
			var customers_name                          string

			row.Scan(
				&id 									,	
				&created_at 							,	
				&resource_type 							,	
				&action 								,	
				&details_origin 						,	
				&details_cause 							,	
				&details_description	        		,	
				&details_scheme 						,	
				&details_reason_code 					,	
				&links_previous_customer_bank_account	,	
				&links_new_customer_bank_account 		,	
				&links_parent_event 					,	
				&links_mandate 							,	
				&mandates_id 							,	
				&mandates_created_at 					,	
				&mandates_reference 					,	
				&mandates_status 						,	
				&mandates_scheme 						,	
				&mandates_next_possible_charge_date   	,	
				&mandates_payments_require_approval   	,	
				&mandates_links_customer_bank_account 	,	
				&mandates_links_creditor 				,	
				&customers_id							,
				&customers_given_name 					,	
				&customers_family_name 					,	
				&customers_company_name 				,	
				&customers_metadata_leadID 				,	
				&customers_metadata_link 				,	
				&customers_metadata_xero 				,	
				&mandates_metadata_xero 				,
				&imported_at                            ,
			    &customers_name                          )
			
			// fields to determine
			var crm_account_number  string
			var crm_id              string
			var crm_name            string
			var crm_email           string
			var crm_premise_address string
			var crm_stage_name      string
			var crm_customer_name   string
			var crm_gocardless_id   string
			var target_team         string
			var crm_zen_user_id     string
			fmt.Println(id, customers_name)

			var found = false

			// Method 1: customers_metadata_leadID is a CRM account number?
			if found == false {
				if customers_metadata_leadID != "" {
					SQLGetCRMAccount := fmt.Sprintf(`SELECT DISTINCT crm_account_number, crm_id, crm_name, crm_email, crm_premise_address, crm_stage_name, crm_gocardless_id, crm_zen_user_id
						FROM crmAccounts 
						WHERE ( crm_id             = "%s"
						  OR    crm_account_number = "%s"
						)`, strings.TrimSpace(customers_metadata_leadID), strings.TrimSpace(customers_metadata_leadID))
					row, err := db.Query(SQLGetCRMAccount)
					if err == nil {
						for row.Next() {
							row.Scan(&crm_account_number,&crm_id,&crm_name,&crm_email,&crm_premise_address,&crm_stage_name,&crm_gocardless_id,&crm_zen_user_id)
							if crm_id != "" || crm_account_number != "" {
								found = true
								break
							}
						}
					} 
					defer row.Close()
				}
				if found {
					fmt.Println("Method 1: customers_metadata_leadID:", customers_metadata_leadID, " found crm_id:", crm_id, " crm_account_number: ", crm_account_number) 
				} else {
					fmt.Println("Method 1: customers_metadata_leadID:", customers_metadata_leadID, " didn't find a crm record", crm_id, crm_account_number)
				}
			}

			// Method 2: mandates_id is valid CRM account number
			if found == false {
				if mandates_id != "" {
					SQLGetCRMAccount := fmt.Sprintf(`SELECT DISTINCT crm_account_number, crm_id, crm_name, crm_email, crm_premise_address, crm_stage_name, crm_gocardless_id, crm_zen_user_id
						FROM elevateAccounts
						INNER JOIN crmAccounts 
						ON elevate_account_number = crm_account_number
						WHERE elevate_mandate_reference = "%s"`, strings.TrimSpace(mandates_id))
					row, err := db.Query(SQLGetCRMAccount)
					if err == nil {
						for row.Next() {
							row.Scan(&crm_account_number,&crm_id,&crm_name,&crm_email,&crm_premise_address,&crm_stage_name,&crm_gocardless_id,&crm_zen_user_id)
							if crm_id != "" || crm_account_number != "" {
								found = true
								break
							}
						}
					} 
					defer row.Close()
				}
				if found {
					fmt.Println("Method 2: mandates_id:", mandates_id, " found crm_id:", crm_id, " crm_account_number: ", crm_account_number) 
				} else {
					fmt.Println("Method 2: mandates_id:", mandates_id, " didn't find a crm record", crm_id, crm_account_number)
				}
			}

			// Match with Method 3: customers_id is valid CRM account number
			if found == false {
				if customers_id != "" {
					SQLGetCRMAccount := fmt.Sprintf(`SELECT DISTINCT crm_account_number, crm_id, crm_name, crm_email, crm_premise_address, crm_stage_name, crm_gocardless_id, crm_zen_user_id
						FROM crmAccounts 
						WHERE crm_gocardless_id = "%s"`, strings.TrimSpace(customers_id))
					row, err := db.Query(SQLGetCRMAccount)
					if err == nil {
						for row.Next() {
							row.Scan(&crm_account_number,&crm_id,&crm_name,&crm_email,&crm_premise_address,&crm_stage_name,&crm_gocardless_id,&crm_zen_user_id)
							if crm_id != "" || crm_account_number != "" {
								found = true
								break
							}
						}
					} 
					defer row.Close()
				}
				if found {
					fmt.Println("Method 3: customers_id:", customers_id, " found crm_id:", crm_id, " crm_account_number: ", crm_account_number) 
				} else {
					fmt.Println("Method 3: customers_id:", customers_id, " didn't find a crm record", crm_id, crm_account_number)
				}
			}

			// Match with Method 4: customers_id is valid CRM account number
			if found == false {
				if customers_name != "" {
					SQLGetCRMAccount := fmt.Sprintf(`SELECT DISTINCT crm_account_number, crm_id, crm_name, crm_email, crm_premise_address, crm_stage_name, crm_gocardless_id, crm_zen_user_id
						FROM crmAccounts 
						WHERE crm_name = "%s"`, strings.TrimSpace(customers_name))
					row, err := db.Query(SQLGetCRMAccount)
					if err == nil {
						for row.Next() {
							row.Scan(&crm_account_number,&crm_id,&crm_name,&crm_email,&crm_premise_address,&crm_stage_name,&crm_gocardless_id,&crm_zen_user_id)
							if crm_id != "" || crm_account_number != "" {
								found = true
								break
							}
						}
					} 
					defer row.Close()
				}
				if found {
					fmt.Println("Method 4: customers_name:", customers_name, " found crm_id:", crm_id, " crm_account_number: ", crm_account_number) 
				} else {
					fmt.Println("Method 4: customers_name:", customers_name, " didn't find a crm record", crm_id, crm_account_number)
				}
			}

			// determine processing team depending on the stage
			switch crm_stage_name {
				case "N/A": 			target_team = "Pre-Installation"
				case "SOLD": 			target_team = "Pre-Installation"
				case "INSTALL":			target_team = "Pre-Installation"
				case "PROVISIONING":	target_team = "Post-Installation"
				case "INVOICING":		target_team = "Post-Installation"
				case "ACTIVE":			target_team = "Post-Installation"
				case "INACTIVE":		target_team = "No action - Inactive"
				default:    			target_team = "Pre-Installation"
			}

			// determine special case for "at your request"
			if strings.Contains(details_description, "at your request") {
				target_team = "No action - at our request"
			}

			resultRow := 	"\"" + id + "\"," +
							"\"" + created_at + "\"," +
							"\"" + resource_type + "\"," +
							"\"" + action + "\"," +
							"\"" + details_origin + "\"," +
							"\"" + details_cause + "\"," +
							"\"" + details_description + "\"," +
							"\"" + details_scheme + "\"," +
							"\"" + details_reason_code + "\"," +
							"\"" + links_previous_customer_bank_account + "\"," +
							"\"" + links_new_customer_bank_account + "\"," +
							"\"" + links_parent_event + "\"," +
							"\"" + links_mandate + "\"," +
							"\"" + mandates_id + "\"," +
							"\"" + mandates_created_at + "\"," +
							"\"" + mandates_reference + "\"," +
							"\"" + mandates_status + "\"," +
							"\"" + mandates_scheme + "\"," +
							"\"" + mandates_next_possible_charge_date + "\"," +
							"\"" + mandates_payments_require_approval + "\"," +
							"\"" + mandates_links_customer_bank_account + "\"," +
							"\"" + mandates_links_creditor + "\"," +
							"\"" + customers_id + "\"," +
							"\"" + customers_given_name + "\"," +
							"\"" + customers_family_name + "\"," +
							"\"" + customers_company_name + "\"," +
							"\"" + customers_metadata_leadID + "\"," +
							"\"" + customers_metadata_link + "\"," +
							"\"" + customers_metadata_xero + "\"," +
							"\"" + mandates_metadata_xero + "\"," +
							"\"" + imported_at + "\"," +
							"\"" + customers_name + "\"," +
							"\"" + crm_account_number + "\"," +
							"\"" + crm_id + "\"," +
							"\"" + crm_name + "\"," +
							"\"" + crm_email + "\"," +
							"\"" + crm_premise_address + "\"," +
							"\"" + crm_stage_name + "\"," +
							"\"" + crm_customer_name + "\"," +
							"\"" + crm_gocardless_id + "\"," +
							"\"" + target_team + "\"," +
							"\"" + crm_zen_user_id  + "\"\n"

				if target_team == "Pre-Installation" {
					if _, err = targetFilePreTeam.WriteString(resultRow); err != nil {
						panic(err)
					}
				} else if target_team == "Post-Installation" {
					if _, err = targetFilePostTeam.WriteString(resultRow); err != nil {
						panic(err)
					}
				} else {
					if _, err = targetFileOthers.WriteString(resultRow); err != nil {
						panic(err)
					}
				}
				fmt.Println(" \n")
	}
	tx.Commit()
	targetFilePreTeam.Close()
	targetFilePostTeam.Close()
	targetFileOthers.Close()
}

func main() {
	var dbName string
	var csvAccountsFrom string
	var csvCRMFrom string
	var csvCancelledFrom string
	var csvFailedFrom string
	var csvPreTeamTo string
	var csvPostTeamTo string
	var csvOtherTeamTo string
	var timestamp = time.Now().Format("2006-01-02")
	var current_path = getCurrentPath()
	var defaultDatabaseName          = filepath.Join( current_path, "cancelled-mandates-database.sqlite3"                  )
	var defaultAccountsFileName      = filepath.Join( current_path, "elevate-accounts-"                               + timestamp + ".csv" )
	var defaultCRMFileName           = filepath.Join( current_path, "crm-accounts-"                                   + timestamp + ".csv" )
	var defaultFromCancelledFileName = filepath.Join( current_path, "cancelled-mandates-"                             + timestamp + ".csv" )
	var defaultFromFailedFileName    = filepath.Join( current_path, "failed-mandates-"                                + timestamp + ".csv" )
	var defaultToPreFileName         = filepath.Join( current_path, "mandates-to-process-by-pre-installation-team-"   + timestamp + ".csv" )
	var defaultToPostFileName        = filepath.Join( current_path, "mandates-to-process-by-post-installation-team-"  + timestamp + ".csv" )
	var defaultToOthersFileName      = filepath.Join( current_path, "mandates-to-check-"                              + timestamp + ".csv" )

	fmt.Println(" ")
	fmt.Println("***********************************************************")
	fmt.Println("PROCESSING CANCELLED MANDATES -- started")
	fmt.Println("***********************************************************")
	
	// get command-line parameters or use defaults
	flag.StringVar(&dbName,               "db",        defaultDatabaseName,          "Sqlite database to import to"    )
	flag.StringVar(&csvAccountsFrom,      "elevate",   defaultAccountsFileName,      "CSV file to import accounts from")
	flag.StringVar(&csvCRMFrom,           "crm",       defaultCRMFileName,           "CSV file to import crm from"     )
	flag.StringVar(&csvCancelledFrom,     "cancelled", defaultFromCancelledFileName, "CSV file to import from"         )
	flag.StringVar(&csvFailedFrom,        "failed",    defaultFromFailedFileName,    "CSV file to import from"         )
	flag.StringVar(&csvPreTeamTo,         "toPre",     defaultToPreFileName,         "CSV file pre-processing-team  to export result to")
	flag.StringVar(&csvPostTeamTo,        "toPost",    defaultToPostFileName,        "CSV file post-processing-team to export result to")
	flag.StringVar(&csvOtherTeamTo,       "toCheck",   defaultToOthersFileName,      "CSV file to-check             to export result to")

	flag.Parse()
	
	if dbName == "" {
		flag.PrintDefaults()
	}
	
	fmt.Println("Received      Database Name        :", dbName)
	fmt.Println("Received CSV-From-File Accounts    :", csvAccountsFrom)
	fmt.Println("Received CSV-From-File CRM         :", csvCRMFrom)
	fmt.Println("Received CSV-From-File Name        :", csvCancelledFrom)
	fmt.Println("Received CSV-From-File Name        :", csvFailedFrom)
	fmt.Println("Received CSV-To-Pre-Team  File Name:", csvPreTeamTo)
	fmt.Println("Received CSV-To-Post-Team File Name:", csvPostTeamTo)
	fmt.Println("Received CSV-To-Check     File Name:", csvOtherTeamTo)
	fmt.Println("***********************************************************")

	db := createDatabase(dbName)
	createTableElevateAccounts(db)
	createTableMandateEvents(db)
	createTableCRMAccounts(db)
	createIndexMandateEventsTimestamp(db)
	createIndexCRMAccountsAccountNumber(db)
	createIndexCRMAccountsName(db)
	createIndexCRMAccountsGoCardlessId(db)
	createIndexElevateAccountsMandateReference(db)
	importElevateAccounts(db, csvAccountsFrom)
	importCRMAccounts(db, csvCRMFrom)
	importMandateEvents(db, csvCancelledFrom)
	importMandateEvents(db, csvFailedFrom)
	processMandateEvents(db, csvPreTeamTo, csvPostTeamTo, csvOtherTeamTo)
	defer db.Close()

	fmt.Println(" ")
	fmt.Println("***********************************************************")
	fmt.Println(" F I N I S H E D")
	fmt.Println("***********************************************************")
}
