/*****************************************************************************************
** TOR History                                                                          **
** (C) Krassimir Tzvetanov                                                              **
** Distributed under Attribution-NonCommercial-ShareAlike 4.0 International             **
** https://creativecommons.org/licenses/by-nc-sa/4.0/legalcode                          **
*****************************************************************************************/

package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"
	"regexp"
	"strings"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
)

type DB struct {
	initialized    bool
	dbh            *sql.DB
	stmtTorQueries *sql.Stmt

	stmtAddTorRelays        *sql.Stmt
	stmtUpdTorRelaysRLS     *sql.Stmt
	stmtLoadLatestTorRelays *sql.Stmt

	// Caches
	lrd map[string](map[string]string) // lrd = latest relay data

	fp2idMap     map[string]string
	region2idMap map[string]string
	city2idMap   map[string]string
	cc2cyNameMap map[string]string

	platform2idMap map[string]string
	version2idMap  map[string]string
	contact2idMap  map[string]string

	exitPol2idMap      map[string]string
	exitPolSum2idMap   map[string]string
	exitPolV6Sum2idMap map[string]string

	// Caches of last item inserted before a timestamp
	latestOr4 map[string](map[string](map[string]string))
	latestOr6 map[string](map[string](map[string]string))
	latestEx4 map[string](map[string](map[string]string))
	latestEx6 map[string](map[string](map[string]string))
	latestDi4 map[string](map[string](map[string]string))
	latestDi6 map[string](map[string](map[string]string))

	// Cache related SQL statements
	stmtAddNodeFingerprints *sql.Stmt
	stmtAddCountryCode      *sql.Stmt
	stmtAddRegion           *sql.Stmt
	stmtAddCity             *sql.Stmt
	stmtAddPlatform         *sql.Stmt
	stmtAddVersion          *sql.Stmt
	stmtAddContact          *sql.Stmt

	// Prepared SQL statements
	stmtGetNodeIdByFp       *sql.Stmt
	stmtGetRegionIdByName   *sql.Stmt
	stmtGetCityIdByName     *sql.Stmt
	stmtGetTorLastTorRelay  *sql.Stmt
	stmtGetVersionIdByName  *sql.Stmt
	stmtGetPlatformIdByName *sql.Stmt
	stmtGetContactIdByName  *sql.Stmt

	stmtAddExitPolicy                  *sql.Stmt
	stmtGetExitPolicyIdByName          *sql.Stmt
	stmtAddExitPolicySummary           *sql.Stmt
	stmtGetExitPolicySummaryIdByName   *sql.Stmt
	stmtAddExitPolicyV6Summary         *sql.Stmt
	stmtGetExitPolicyV6SummaryIdByName *sql.Stmt

	stmtAddOrV4   *sql.Stmt
	stmtAddExitV4 *sql.Stmt
	stmtAddDirV4  *sql.Stmt
	stmtAddOrV6   *sql.Stmt
	stmtAddExitV6 *sql.Stmt
	stmtAddDirV6  *sql.Stmt

	stmtUpdOr4RLS *sql.Stmt
	stmtUpdEx4RLS *sql.Stmt
	stmtUpdDi4RLS *sql.Stmt
	stmtUpdOr6RLS *sql.Stmt
	stmtUpdEx6RLS *sql.Stmt
	stmtUpdDi6RLS *sql.Stmt
}

//***************************************************************************
// Open/Initialize/Close functions

// Factory creating a new DB
// Construct connection string from tokens and execute "Open()"
// Initialize prepared statements
func NewDB(Username string, Password string, Host string, Port string, DBName string) *DB {
	ifPrintln(1, "Initializing database...")
	defer ifPrintln(1, "Database Ready.")
	var db DB
	var err error

	conString := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", Username, Password, Host, Port, DBName)

	db.dbh, err = sql.Open("mysql", conString)
	if err != nil {
		panic("func NewDB: " + err.Error())
	}

	// Prepare various SQL queries
	SQLStatements := map[string]**sql.Stmt{
		"INSERT INTO Countries (CC, CountryName) VALUES( ?, ?)":                                                           &db.stmtAddCountryCode,
		"INSERT INTO TorQueries (Version, Relays_published, Bridges_published, AcquisitionTimestamp) VALUES( ?, ?, ?, ?)": &db.stmtTorQueries,

		"INSERT INTO TorRelays (ID_NodeFingerprints, ID_Countries, ID_Regions, ID_Cities, ID_Platforms, ID_Versions, ID_Contacts, " +
			"ID_ExitPolicies, ID_ExitPolicySummaries, ID_ExitPolicyV6Summaries, Nickname, Last_changed_address_or_port, First_seen, RecordTimeInserted, RecordLastSeen, flags, jsd) " +
			"VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);": &db.stmtAddTorRelays,

		"UPDATE TorRelays SET RecordLastSeen = ? WHERE ID = ?;": &db.stmtUpdTorRelaysRLS,

		"INSERT INTO NodeFingerprints (Fingerprint) VALUES( ?)":  &db.stmtAddNodeFingerprints,
		"SELECT ID FROM NodeFingerprints WHERE Fingerprint = ?;": &db.stmtGetNodeIdByFp,
		"INSERT INTO Cities (CityName) VALUES( ?)":               &db.stmtAddCity,
		"SELECT ID FROM Cities WHERE CityName = ?;":              &db.stmtGetCityIdByName,
		"INSERT INTO Regions (RegionName) VALUES( ?)":            &db.stmtAddRegion,
		"SELECT ID FROM Regions WHERE RegionName = ?;":           &db.stmtGetRegionIdByName,

		"INSERT INTO Platforms (PlatformName) VALUES( ?)":  &db.stmtAddPlatform,
		"SELECT ID FROM Platforms WHERE PlatformName = ?;": &db.stmtGetPlatformIdByName,
		"INSERT INTO Versions (VersionName) VALUES( ?)":    &db.stmtAddVersion,
		"SELECT ID FROM Versions WHERE VersionName = ?;":   &db.stmtGetVersionIdByName,
		"INSERT INTO Contacts (ContactName) VALUES( ?)":    &db.stmtAddContact,
		"SELECT ID FROM Contacts WHERE ContactName = ?;":   &db.stmtGetContactIdByName,

		"INSERT INTO ExitPolicies (ExitPolicy) VALUES( ?)":                    &db.stmtAddExitPolicy,
		"SELECT ID FROM ExitPolicies WHERE ExitPolicy = ?;":                   &db.stmtGetExitPolicyIdByName,
		"INSERT INTO ExitPolicySummaries (ExitPolicySummary) VALUES( ?)":      &db.stmtAddExitPolicySummary,
		"SELECT ID FROM ExitPolicySummaries WHERE ExitPolicySummary = ?;":     &db.stmtGetExitPolicySummaryIdByName,
		"INSERT INTO ExitPolicyV6Summaries (ExitPolicyV6Summary) VALUES( ?)":  &db.stmtAddExitPolicyV6Summary,
		"SELECT ID FROM ExitPolicyV6Summaries WHERE ExitPolicyV6Summary = ?;": &db.stmtGetExitPolicyV6SummaryIdByName,

		"INSERT INTO Or_addresses_v4 (ID_NodeFingerprints, RecordTimeInserted, RecordLastSeen, ip4, port) VALUES(?, ?, ?, INET_ATON(?), ?)":  &db.stmtAddOrV4,
		"INSERT INTO Exit_addresses_v4 (ID_NodeFingerprints, RecordTimeInserted, RecordLastSeen, ip4) VALUES(?, ?, ?, INET_ATON(?))":         &db.stmtAddExitV4,
		"INSERT INTO Dir_addresses_v4 (ID_NodeFingerprints, RecordTimeInserted, RecordLastSeen, ip4, port) VALUES(?, ?, ?, INET_ATON(?), ?)": &db.stmtAddDirV4,

		"INSERT INTO Or_addresses_v6 (ID_NodeFingerprints, RecordTimeInserted, RecordLastSeen, ip6, port) VALUES(?, ?, ?, INET6_ATON(?), ?)":  &db.stmtAddOrV6,
		"INSERT INTO Exit_addresses_v6 (ID_NodeFingerprints, RecordTimeInserted, RecordLastSeen, ip6) VALUES(?, ?, ?, INET6_ATON(?))":         &db.stmtAddExitV6,
		"INSERT INTO Dir_addresses_v6 (ID_NodeFingerprints, RecordTimeInserted, RecordLastSeen, ip6, port) VALUES(?, ?, ?, INET6_ATON(?), ?)": &db.stmtAddDirV6,

		"UPDATE Or_addresses_v4 SET RecordLastSeen=? WHERE ID = ?":   &db.stmtUpdOr4RLS,
		"UPDATE Exit_addresses_v4 SET RecordLastSeen=? WHERE ID = ?": &db.stmtUpdEx4RLS,
		"UPDATE Dir_addresses_v4 SET RecordLastSeen=? WHERE ID = ?":  &db.stmtUpdDi4RLS,

		"UPDATE Or_addresses_v6 SET RecordLastSeen=? WHERE ID = ?;":   &db.stmtUpdOr6RLS,
		"UPDATE Exit_addresses_v6 SET RecordLastSeen=? WHERE ID = ?;": &db.stmtUpdEx6RLS,
		"UPDATE Dir_addresses_v6 SET RecordLastSeen=? WHERE ID = ?;":  &db.stmtUpdDi6RLS,
	}

	for stmt, storage := range SQLStatements {
		*storage, err = db.dbh.Prepare(stmt)
		if err != nil {
			fmt.Println("PANIC: SQL Statement: " + stmt)
			panic("func NewDB: for: " + err.Error())
		}
	}

	db.initialized = true
	return &db
}

// Take Config object and convert it in a way consumable for the previous NewDB
func NewDBFromConfig(cfg TorHistoryConfig) *DB {
	return NewDB(cfg.DBServer.Username, cfg.DBServer.Password, cfg.DBServer.Host, cfg.DBServer.Port, cfg.DBServer.DBName)
}

/*
func IPtoRelayIDs(type string, ip46 string) []TorRelayDetails{
// Given IP address and function (Or, Ex, Di), produces a list of nodes which occupied used that IP for that function
// Since multiple relays may have used that IP it returns a list of TorRelay Nodes.
// Maybe collect the time periods?
	switch type {
	case "Or":
		break
	}

}
*/

/*
func assembleRecordByIPID (Or, Ex, Di){
	// Detect v4/v6
	// Add time fram limitations
	switch type {
	case "Or":
		"SELECT o4.ID, o4.ID_NodeFingerprints FROM Or_addresses_v4 o4 LEFT JOIN TorRelays tr ON o4.ID_NodeFingerprints = tr.ID_NodeFingerprints WHERE o4.ID = ?;", qID);
		break
	case "Ex":
		break
	case "Di":
		break

	}
}

func assembleRecordByTorRelayID(id uint) *TorRelayDetails {

	if !db.initialized {
		log.Fatal("Call to a method in uninitialized database initializeLatestRelayDataCache.")
	}
	*lrd = g_db.SQLQueryTYPEOfMaps("mapOfMaps",
		`SELECT Fingerprint, tr.ID id, Nickname, RecordTimeInserted, DATE_FORMAT( RecordLastSeen, "%Y%m%d%H%i%s") as RecordLastSeen,
			ID_Countries Country, CityName, PlatformName, VersionName, ContactName, First_seen, Last_changed_address_or_port,
			ExitPolicy, ExitPolicySummary, ExitPolicyV6Summary, tr.ID_Versions, tr.ID_Contacts, ID_NodeFingerprints
			FROM TorRelays tr
			LEFT JOIN NodeFingerprints nf ON tr.ID_NodeFingerprints = nf.ID
			LEFT JOIN Cities c ON ID_Cities = c.ID
			LEFT JOIN Platforms p ON ID_Platforms = p.ID
			LEFT JOIN Versions v ON ID_Versions = v.ID
			LEFT JOIN Contacts ct ON ID_Contacts = ct.ID
			LEFT JOIN ExitPolicies ep ON ID_ExitPolicies = ep.ID
			LEFT JOIN ExitPolicySummaries eps ON ID_ExitPolicySummaries = eps.ID
			LEFT JOIN ExitPolicyV6Summaries eps6 ON ID_ExitPolicyV6Summaries = eps6.ID
			WHERE (ID_NodeFingerprints, RecordLastSeen) IN
			(SELECT ID_NodeFingerprints, max(RecordLastSeen) FROM TorRelays WHERE RecordLastSeen <= "`+cdts+`" GROUP BY ID_NodeFingerprints);`).(map[string](map[string]string))

}
*/
func (db *DB) initializeLatestRelayDataCache(lrd *map[string](map[string]string), cdts string) { // cdts generally is g_consensusDLTS
	ifPrintln(3, "Initializing Latest Relay Data (LRD) cache...")
	defer ifPrintln(3, "Latest Relay Data (LRD) cache ready.")

	if !db.initialized {
		log.Fatal("Call to a method in uninitialized database initializeLatestRelayDataCache.")
	}
	*lrd = g_db.SQLQueryTYPEOfMaps("mapOfMaps",
		`SELECT Fingerprint, tr.ID id, Nickname, RecordTimeInserted, DATE_FORMAT( RecordLastSeen, "%Y%m%d%H%i%s") as RecordLastSeen, 
			ID_Countries Country, CityName, PlatformName, VersionName, ContactName, First_seen, Last_changed_address_or_port, 
			ExitPolicy, ExitPolicySummary, ExitPolicyV6Summary, tr.ID_Versions, tr.ID_Contacts, ID_NodeFingerprints
			FROM TorRelays tr
			LEFT JOIN NodeFingerprints nf ON tr.ID_NodeFingerprints = nf.ID 
			LEFT JOIN Cities c ON ID_Cities = c.ID
			LEFT JOIN Platforms p ON ID_Platforms = p.ID
			LEFT JOIN Versions v ON ID_Versions = v.ID
			LEFT JOIN Contacts ct ON ID_Contacts = ct.ID
			LEFT JOIN ExitPolicies ep ON ID_ExitPolicies = ep.ID
			LEFT JOIN ExitPolicySummaries eps ON ID_ExitPolicySummaries = eps.ID
			LEFT JOIN ExitPolicyV6Summaries eps6 ON ID_ExitPolicyV6Summaries = eps6.ID
			WHERE (ID_NodeFingerprints, RecordLastSeen) IN 
			(SELECT ID_NodeFingerprints, max(RecordLastSeen) FROM TorRelays WHERE RecordLastSeen <= "`+cdts+`" GROUP BY ID_NodeFingerprints);`).(map[string](map[string]string))
}

func (db *DB) initCaches() {
	ifPrintln(3, "initCaches: Initialiazing memory caches from database...")
	defer ifPrintln(3, "Caches ready.")
	if !db.initialized {
		log.Fatal("Call to a method in uninitialized database" + "initCaches" + ".")
	}
	db.fp2idMap = db.SQLQueryKeyValue("SELECT Fingerprint, ID FROM NodeFingerprints;")
	db.region2idMap = db.SQLQueryKeyValue("SELECT RegionName, ID FROM Regions;")
	db.city2idMap = db.SQLQueryKeyValue("SELECT CityName, ID FROM Cities;")
	db.platform2idMap = db.SQLQueryKeyValue("SELECT PlatformName, ID FROM Platforms;")
	db.version2idMap = db.SQLQueryKeyValue("SELECT VersionName, ID FROM Versions;")
	db.contact2idMap = db.SQLQueryKeyValue("SELECT ContactName, ID FROM Contacts;")

	db.exitPol2idMap = db.SQLQueryKeyValue("SELECT ExitPolicy, ID FROM ExitPolicies;")
	db.exitPolSum2idMap = db.SQLQueryKeyValue("SELECT ExitPolicySummary, ID FROM ExitPolicySummaries;")
	db.exitPolV6Sum2idMap = db.SQLQueryKeyValue("SELECT ExitPolicyV6Summary, ID FROM ExitPolicyV6Summaries;")

	// Load latest records BEFORE the current insert timestamp. Note this allows us to insert older data files (retroactively)
	db.latestOr4 = db.SQLQueryTYPEOfMaps("mapOfMapOfMaps", "SELECT ID_NodeFingerprints, INET_NTOA(ip4) ip4, ID, DATE_FORMAT( RecordLastSeen, '%Y%m%d%H%i%s') as RecordLastSeen, port "+
		"FROM Or_addresses_v4 WHERE (ID_NodeFingerprints, RecordLastSeen) IN "+
		"(SELECT ID_NodeFingerprints, max(RecordLastSeen) FROM Or_addresses_v4 WHERE RecordLastSeen <= "+g_consensusDLTS+
		" GROUP BY ID_NodeFingerprints);").(map[string](map[string](map[string]string)))

	db.latestOr6 = db.SQLQueryTYPEOfMaps("mapOfMapOfMaps", "SELECT ID_NodeFingerprints, INET6_NTOA(ip6) ip6, ID, DATE_FORMAT( RecordLastSeen, '%Y%m%d%H%i%s') as RecordLastSeen, port FROM Or_addresses_v6 "+
		"WHERE (ID_NodeFingerprints, RecordLastSeen) IN (SELECT ID_NodeFingerprints, max(RecordLastSeen) FROM Or_addresses_v4 "+
		"WHERE RecordLastSeen <= "+g_consensusDLTS+" GROUP BY ID_NodeFingerprints);").(map[string](map[string](map[string]string)))

	db.latestEx4 = db.SQLQueryTYPEOfMaps("mapOfMapOfMaps", "SELECT ID_NodeFingerprints, INET_NTOA(ip4) ip4, ID, DATE_FORMAT( RecordLastSeen, '%Y%m%d%H%i%s') as RecordLastSeen FROM Exit_addresses_v4 "+
		"WHERE (ID_NodeFingerprints, RecordLastSeen) IN (SELECT ID_NodeFingerprints, max(RecordLastSeen) "+
		"FROM Exit_addresses_v4 WHERE RecordLastSeen <= "+g_consensusDLTS+" GROUP BY ID_NodeFingerprints);").(map[string](map[string](map[string]string)))

	db.latestEx6 = db.SQLQueryTYPEOfMaps("mapOfMapOfMaps", "SELECT ID_NodeFingerprints, INET6_NTOA(ip6) ip6, ID, DATE_FORMAT( RecordLastSeen, '%Y%m%d%H%i%s') as RecordLastSeen FROM Exit_addresses_v6 "+
		"WHERE (ID_NodeFingerprints, RecordLastSeen) IN (SELECT ID_NodeFingerprints, max(RecordLastSeen) FROM Exit_addresses_v6 "+
		"WHERE RecordLastSeen <= "+g_consensusDLTS+" GROUP BY ID_NodeFingerprints);").(map[string](map[string](map[string]string)))

	db.latestDi4 = db.SQLQueryTYPEOfMaps("mapOfMapOfMaps", "SELECT ID_NodeFingerprints, INET_NTOA(ip4) ip4, ID, DATE_FORMAT( RecordLastSeen, '%Y%m%d%H%i%s') as RecordLastSeen, port FROM Dir_addresses_v4 "+
		"WHERE (ID_NodeFingerprints, RecordLastSeen) IN (SELECT ID_NodeFingerprints, max(RecordLastSeen) FROM Dir_addresses_v4 "+
		"WHERE RecordLastSeen <= "+g_consensusDLTS+" GROUP BY ID_NodeFingerprints);").(map[string](map[string](map[string]string)))

	db.latestDi6 = db.SQLQueryTYPEOfMaps("mapOfMapOfMaps", "SELECT ID_NodeFingerprints, INET6_NTOA(ip6) ip6, ID, DATE_FORMAT( RecordLastSeen, '%Y%m%d%H%i%s') as RecordLastSeen, port FROM Dir_addresses_v6 "+
		"WHERE (ID_NodeFingerprints, RecordLastSeen) IN (SELECT ID_NodeFingerprints, max(RecordLastSeen) FROM Dir_addresses_v4 "+
		"WHERE RecordLastSeen <= "+g_consensusDLTS+" GROUP BY ID_NodeFingerprints);").(map[string](map[string](map[string]string)))
	ifPrintln(2, "initCaches: Caches initialized")
}

func (db *DB) initCountryNameCache() {
	ifPrintln(3, "Initializing Countryname cache...")
	defer ifPrintln(3, "Countryname cache ready.")

	if !db.initialized {
		log.Fatal("Call to a method in uninitialized database" + "initCountryNameCache" + ".")
	}
	ifPrintln(2, "initCountryNameCache: Initialiazing memocountry codes cache from database")
	db.cc2cyNameMap = db.SQLQueryKeyValue("SELECT LOWER(CC) CC, CountryName FROM Countries;") // Uses LOWER() just in case the database was initialized with capital CC
}

func (db *DB) Close() {
	if db == nil || !db.initialized {
		return
	}
	ifPrintln(8, "Database shutdown")
	ifPrintln(4, "Closing statement and connection.")
	db.dbh.Close()
	db.stmtTorQueries.Close()
}

//***************************************************************************
// SQL query functions

// Executes an arbitrary SQL query which return two columns and returns a map
// where the first column is the key and second the value
func (db *DB) SQLQueryKeyValue(query string) map[string]string {
	if !db.initialized {
		log.Fatal("Call to a method in uninitialized database" + "SQLQueryKeyValue" + ".")
	}
	ifPrintln(5, "SQLQueryKeyValue("+db.escapePercentSign(query)+"): ")
	rows, err := db.dbh.Query(query)
	if err != nil {
		panic("func SQLQueryKeyValue: " + err.Error())
	}

	// Verify the return column count
	columns, err := rows.Columns()
	if err != nil || len(columns) != 2 {
		panic("func SQLQueryKeyValue: " + fmt.Sprintf("ERROR: Columns returned %d.\n", len(columns)) + err.Error())
	}

	// Allocate storage for the result, returned to the caller
	var resultMap = make(map[string]string)

	// Buffer variables
	var key, val string

	for rows.Next() {
		err = rows.Scan(&key, &val)
		if err != nil {
			panic("func SQLQueryKeyValue: " + err.Error())
		}
		resultMap[key] = val
	}
	ifPrintln(5, "func SQLQueryKeyValue RETURN a map (not expanded)")
	return resultMap
}

// Returs the query as a map or slice of maps, depending on the TYPE argument
// The key for the outer map is the first element in the SELECT query
// The inner maps contain the full record, including the first element
// TYPE one of:
//		sliceOfMaps:	[](map[string]string)
//		mapOfMaps:		map[string](map[string]string)
//		mapOfMapOfMaps: map[string](map[string](map[string]string))
//		sliceOfSlice
func (db *DB) SQLQueryTYPEOfMaps(TYPE string, query string) interface{} {
	ifPrintln(5, "func SQLQueryTYPEOfMaps: ("+TYPE+", \n"+db.escapePercentSign(query)+"):")
	if !db.initialized {
		log.Fatal("Call to a method in uninitialized database.")
	}
	if TYPE != "sliceOfMaps" && TYPE != "mapOfMaps" && TYPE != "mapOfMapOfMaps" && TYPE != "sliceOfSlice" {
		panic("SQLQueryTYPEOfMaps: Supplied TYPE='" + TYPE + "' TYPE can be only one of the following: sliceOfMaps, mapOfMapOfMaps, mapOfMaps, sliceOfSlice")
	}

	rows, err := db.dbh.Query(query)
	if err != nil {
		panic("func SQLQueryTYPEOfMaps: " + err.Error())
	}

	// Figure out how many columns are in the response
	columns, err := rows.Columns()
	if err != nil {
		panic("func SQLQueryTYPEOfMaps: " + err.Error()) // TODO
	}
	ifPrintln(6, fmt.Sprintf("Number of columns returned: %d", len(columns)))

	// Allocate row buffer for each column of type sql.RawBytes
	// Data from rows.scan will be stored there
	rowBuffer := make([]sql.RawBytes, len(columns))

	// Allocate column number of pointers to point to each of the buffers from above
	// This is what we'll pass to rows.scan()
	rowBufferPtrs := make([]interface{}, len(rowBuffer))
	for i := range rowBuffer {
		rowBufferPtrs[i] = &rowBuffer[i]
	}

	// Return result buffers
	result_slice := make([](map[string]string), 0)
	result_map := make(map[string](map[string]string))
	result_map_map := make(map[string](map[string](map[string]string)))

	var result_row map[string]string
	for rows.Next() {
		err = rows.Scan(rowBufferPtrs...)
		if err != nil {
			panic("func SQLQueryTYPEOfMaps: " + err.Error())
		}
		result_row = make(map[string]string)
		for i := 0; i < len(columns); i++ {
			result_row[columns[i]] = string(rowBuffer[i])
		}
		if TYPE == "sliceOfMaps" {
			result_slice = append(result_slice, result_row)
		} else if TYPE == "mapOfMaps" {
			result_map[string(rowBuffer[0])] = result_row
		} else if TYPE == "mapOfMapOfMaps" { // double nested map
			if result_map_map[string(rowBuffer[0])] == nil {
				// Allocating memory for sub-maps for each of the main keys
				result_map_map[string(rowBuffer[0])] = make(map[string](map[string]string))
			}
			result_map_map[string(rowBuffer[0])][string(rowBuffer[1])] = result_row
		} else { // later: TYPE != "sliceOfSlice"
			panic("func SQLQueryTYPEOfMaps() default case")
		}
	}

	if TYPE == "sliceOfMaps" {
		return result_slice
	} else if TYPE == "mapOfMaps" {
		return result_map
	} else if TYPE == "mapOfMapOfMaps" {
		return result_map_map
	} else { // later: TYPE != "sliceOfSlice"
		return nil
	}
}

// Generic function which gets the ID column from one of the caches/indexes by its value
func (db *DB) dbGetKeyByValue(valueType string, value string) string {
	ifPrintln(6, "func dbGetKeyByValue("+valueType+"): "+value)
	if !db.initialized {
		log.Fatal("Call to a method in uninitialized database.")
	}
	var err error
	var row *sql.Rows

	switch valueType {
	case "fingerprint":
		row, err = db.stmtGetNodeIdByFp.Query(value)
		//	case "country":
		//		res, err = db.stmtGetNodeIdByFp.Query(value)
	case "region":
		row, err = db.stmtGetRegionIdByName.Query(value)
	case "city":
		row, err = db.stmtGetCityIdByName.Query(value)
	case "platform":
		row, err = db.stmtGetPlatformIdByName.Query(value)
	case "version":
		row, err = db.stmtGetVersionIdByName.Query(value)
	case "contact":
		row, err = db.stmtGetContactIdByName.Query(value)
	case "exitp":
		row, err = db.stmtGetExitPolicyIdByName.Query(value)
	case "exitps":
		row, err = db.stmtGetExitPolicySummaryIdByName.Query(value)
	case "exitps6":
		row, err = db.stmtGetExitPolicyV6SummaryIdByName.Query(value)
	default:
		panic("func dbGetKeyByValue: Invalid key/value type: " + valueType)
	}

	if err != nil {
		panic("func dbGetKeyByValue: " + err.Error())
	}

	var id string
	if row.Next() {
		row.Scan(&id)
	}

	if row.Next() { // We have a problem if more than one lines match the fingerprint
		// for now gracefully ignore - otherwise it should bounce it here
		ifPrintln(-1, fmt.Sprintf("dbGetKeyByValue: MORE THAN ONE FINGERPRINTES RETURNED for %s.", value))
	}
	return id
}

/*
// Performance optimized to get a Fingerprint ID by the Fingerprint
func (db *DB) GetNodeIdByFingerprint(fp string) string {
	row, err := db.stmtGetNodeIdByFp.Query(fp)
	if err != nil {
		panic(err.Error())
	}

	var id string
	if row.Next() {
		row.Scan(&id)
	}

	if row.Next() { // We have a problem if more than one lines match the fingerprint
		ifPrintln(3, fmt.Sprintf("MORE THAN ONE FINGERPRINTES RETURNED for %s.", fp))
		//gracefully ignore - otherwise it should bounce it here
	}
	return id
}*/

func (db *DB) addToTorQueries(version string, relays_published string, bridges_published string, acquisition_ts string) {
	ifPrintln(4, "func addToTorQueries("+version+", "+relays_published+","+bridges_published+","+acquisition_ts+")")
	if !db.initialized {
		log.Fatal("Call to a method in uninitialized database.")
	}
	_, err := db.stmtTorQueries.Exec(version, relays_published, bridges_published, acquisition_ts)
	if err != nil {
		fmt.Println("SQL Query broke:")
		fmt.Println(db.stmtTorQueries)
		fmt.Printf("%s, %s, %s, %s\n", version, relays_published, bridges_published, acquisition_ts)
		panic("func addToTorQueries: " + err.Error())
	}
	//lastID_int64, err := res.LastInsertId()
	//	lastID = fmt.Sprintf("%d", lastID_int64)
}

func ipPort(input string) (string, string) {
	//ifPrintln(8, "func ipPort("+input+")")
	var ip, port string
	if input[0] == '[' { // IPv6
		ip6AndPort := strings.SplitN(input, "]", 2)
		checkIP := net.ParseIP(ip6AndPort[0][1:])
		if checkIP == nil {
			log.Fatal("ipPort: problem parsing IPv6: " + ip)
		}
		ip = checkIP.String()

		if !strings.Contains(ip, "::") && strings.Contains(ip, ":0:") { // Normalized already
			re := regexp.MustCompile(`:0:`)
			matches := re.FindAllStringSubmatchIndex(ip, 1)
			if len(matches) > 0 {
				last := matches[0]
				ip = ip[:last[0]+1] + ip[last[1]-1:]
			}
		}

		if len(ip6AndPort) >= 2 {
			port = ip6AndPort[1][1:]
		}
	} else {
		ip4AndPort := strings.SplitN(input, ":", 2)
		ip = ip4AndPort[0]
		if len(ip4AndPort) >= 2 {
			port = ip4AndPort[1]
		}
	}
	return ip, port
}

func (db *DB) addToIP(table string, fpid string, tsIns string, tsRls string, ipAndPort string) {
	ifPrintln(6, "func addToIP(type="+table+"): "+ipAndPort)
	if !db.initialized {
		log.Fatal("Call to a method in uninitialized database.")
	}
	defer ifPrintln(6, "addToIP: RETURN")

	if len(ipAndPort) == 0 {
		ifPrintln(6, "Empty IP/port")
		return
	}

	// Check if it is IPv4 or IPv6
	var stmt *sql.Stmt
	if ipAndPort[0] == '[' { // IPv6
		switch table {
		case "Or":
			stmt = db.stmtAddOrV6
			break
		case "Ex":
			stmt = db.stmtAddExitV6
			break
		case "Di":
			stmt = db.stmtAddDirV6
			break
		default:
			log.Fatal("Reached unexpected case (" + table + ") in IPv6 switch for (" + ipAndPort + ") in addToIP().")
		}
	} else {
		switch table {
		case "Or":
			stmt = db.stmtAddOrV4
			break
		case "Ex":
			stmt = db.stmtAddExitV4
			break
		case "Di":
			stmt = db.stmtAddDirV4
			break
		default:
			log.Fatal("Reached unexpected case (" + table + ") in IPv4 switch for (" + ipAndPort + ") in addToIP().")
		}
	}
	var err error
	if table == "Ex" {
		ip := ipAndPort
		_, err = stmt.Exec(fpid, tsIns, tsRls, ip)
	} else {
		ip, port := ipPort(ipAndPort)
		_, err = stmt.Exec(fpid, tsIns, tsRls, ip, port)
	}
	if err != nil {
		panic("func addToIP(" + table + "): " + err.Error())
	}
}

func (db *DB) updateIfNeededRelayAddressRLS(table string, fpid string, tsRls string, or string) {
	ifPrintln(4, fmt.Sprintf("func updateIfNeededRelayAddressRLS: %s, %s, %s, %s", table, fpid, tsRls, or))
	defer ifPrintln(4, "func updateIfNeededRelayAddressRLS: RETURN")

	if !db.initialized {
		log.Fatal("Call to a method in uninitialized database.")
	}
	ip, port := ipPort(or)
	var rec (map[string]string)
	var updStmt *sql.Stmt
	if or[0] == '[' { // IPv6
		switch table {
		case "Or":
			rec = db.latestOr6[fpid][ip]
			updStmt = db.stmtUpdOr6RLS
			break
		case "Ex":
			rec = db.latestEx6[fpid][ip]
			updStmt = db.stmtUpdEx6RLS
			break
		case "Di":
			rec = db.latestDi6[fpid][ip]
			updStmt = db.stmtUpdEx6RLS
			break
		default:
			panic("updateIfNeededRelayAddressRLS: V6 swtch/default: ")
		}
	} else {
		switch table {
		case "Or":
			rec = db.latestOr4[fpid][ip]
			updStmt = db.stmtUpdOr4RLS
			break
		case "Ex":
			rec = db.latestEx4[fpid][ip]
			updStmt = db.stmtUpdEx4RLS
			break
		case "Di":
			rec = db.latestDi4[fpid][ip]
			updStmt = db.stmtUpdDi4RLS
			break
		default:
			panic("updateIfNeededRelayAddressRLS: V4 swtch/default: ")
		}
	}

	if rec["port"] == port {
		if rec["RecordLastSeen"] == tsRls {
			ifPrintln(5, "func : COMPLETE MATCH: no need to update RLS for: "+tsRls+"; "+rec["RecordLastSeen"]+"; ")
		} else {
			ifPrintln(5, fmt.Sprintf("func updateIfNeededRelayAddressRLS: Updating RLS in %s. Rec id: %s. New time: %s", table, rec["ID"], tsRls))
			_, err := updStmt.Exec(tsRls, rec["ID"])
			if err != nil {
				panic("func updateIfNeededRelayAddressRLS: " + err.Error())
			}
		}
	} else {
		ifPrintln(5, fmt.Sprintf("func updateIfNeededRelayAddressRLS: %s new IP for %s: Inserting %s in DB and cache", fpid, table, or))

		// Adds IP to the corresponding Or, Exor Di table specified in table
		g_db.addToIP(table, fpid, g_consensusDLTS, g_consensusDLTS, or)
	}
}

func (db *DB) updateTorRelayRLS(id string, newTS string) {
	ifPrintln(4, "updateTorRelayRLS: id: "+id+"; new timestamp: "+newTS)
	defer ifPrintln(4, "updateTorRelayRLS: success")

	if !db.initialized {
		log.Fatal("Call to a method in uninitialized database.")
	}

	_, err := db.stmtUpdTorRelaysRLS.Exec(newTS, id)
	if err != nil {
		panic("func updateTorRelayRLS: " + err.Error())
	}
}

//***************************************************************************
// Add key/value variations

func (db *DB) addKeyValue_CC(cc string, country_name string) string {
	ifPrintln(4, "func addKeyValue_CC("+cc+", "+country_name+"): ")
	if !db.initialized {
		log.Fatal("Call to a method in uninitialized database.")
	}
	if len(cc) == 0 || len(country_name) == 0 {
		return "" // Prevent this from causing a DB error
	}
	return db.addKeyValue_real("country", cc, country_name)
}

func (db *DB) addKeyValue(valueType string, value string) string {
	ifPrintln(4, "func addKeyValue("+valueType+", "+value+"): ")
	if !db.initialized {
		log.Fatal("Call to a method in uninitialized database.")
	}
	return db.addKeyValue_real(valueType, value, "")
}

func (db *DB) addKeyValue_real(valueType string, value string, id string) string {
	ifPrintln(4, "func addKeyValue_real("+valueType+", "+value+", "+id+"): ")
	if !db.initialized {
		log.Fatal("Call to a method in uninitialized database.")
	}
	var lastID string
	var err error
	var res sql.Result
	var cache *map[string]string

	// Note that the caches are updated prior to the actual DB transaction and return code
	// verification. That is OK because in case of a DB transaction failure the program would exit
	switch valueType {
	case "fingerprint":
		res, err = db.stmtAddNodeFingerprints.Exec(value)
		cache = &db.fp2idMap
	case "country":
		res, err = db.stmtAddCountryCode.Exec(value, id) // value is CC, id is Country_name
		cache = &db.cc2cyNameMap
	case "region":
		res, err = db.stmtAddRegion.Exec(value)
		cache = &db.region2idMap
	case "city":
		res, err = db.stmtAddCity.Exec(value)
		cache = &db.city2idMap
	case "platform":
		res, err = db.stmtAddPlatform.Exec(value)
		cache = &db.platform2idMap
	case "version":
		res, err = db.stmtAddVersion.Exec(value)
		cache = &db.version2idMap
	case "contact":
		res, err = db.stmtAddContact.Exec(value)
		cache = &db.contact2idMap
	case "exitp":
		res, err = db.stmtAddExitPolicy.Exec(value)
		cache = &db.exitPol2idMap
	case "exitps":
		res, err = db.stmtAddExitPolicySummary.Exec(value)
		cache = &db.exitPolSum2idMap
	case "exitps6":
		res, err = db.stmtAddExitPolicyV6Summary.Exec(value)
		cache = &db.exitPolV6Sum2idMap
	default:
		panic("func addKeyValue_real: Invalid key/value type: " + valueType)
	}

	if err != nil {
		errDetail, _ := err.(*mysql.MySQLError)
		ifPrintln(4, "Add to "+valueType+": SQL insert error")
		switch errDetail.Number {
		case 1062: // Error 1062 means duplicate fingerprint (normal after second run)
			ifPrintln(6, "DETECTED A DUPLICATE (MySQL code 1062)")
			break
		case 1406: // Error 1406: Data too long for column 'xxx' at row 1
			//ifPrintln(-2, "DB field truncated (MySQL code 1406)")
			panic("func addKeyValue_real: (" + valueType + ") " + value + ": => DB field truncated (MySQL code 1406)\n" + err.Error())
			break
		default:
			panic("func addKeyValue_real: (" + valueType + ") " + value + ":\n" + err.Error())
			break
		}

		// Note before this function is called fp2id would have checked the cache
		lastID = db.dbGetKeyByValue(valueType, value)
		ifPrintln(4, fmt.Sprint("LastID (duplicate): %s", lastID))
	} else {
		if valueType != "country" {
			lastID_int64, _ := res.LastInsertId()
			lastID = fmt.Sprintf("%d", lastID_int64)
		} else {
			lastID = value
		}
		(*cache)[value] = lastID
		ifPrintln(4, fmt.Sprintf("LastID (new insert): %s", lastID))
		if err != nil {
			panic("func addKeyValue_real: " + err.Error())
		}
	}
	ifPrintln(4, "func addKeyValue_real: RETURN: "+lastID)
	return lastID
}

//***************************************************************************
// ID lookup functions

func (db *DB) cc2countryName(cc string) string {
	ifPrintln(4, "func cc2countryName("+cc+")")
	if !db.initialized {
		log.Fatal("Call to a method in uninitialized database.")
	}
	// If fingerprint is in the cache already, return it.
	if value, ok := db.cc2cyNameMap[cc]; ok {
		ifPrintln(4, fmt.Sprintf("Cache hit for cc %s, returning %d.", cc, value))
		return value
	} else {
		return ""
	}
}

// If the value is in the corresponding cache, return it.
// If not in the cache, update the cache, enter in the DB and return the DB id
func (db *DB) value2id(valueType string, value string) string {
	ifPrintln(4, "func value2id("+valueType+", "+value+")")
	if !db.initialized {
		log.Fatal("Call to a method in uninitialized database.")
	}
	var ok bool
	var id string
	var cache *map[string]string

	switch valueType {
	case "fingerprint":
		cache = &db.fp2idMap
		break
	case "country": // this is a VERY SPECIAL case
		// For countries we do not need to do a cache resolution as the
		// ##### value = strings.ToLower(value) // Ensure there are not small letter coming from the Consensus
		cache = &db.cc2cyNameMap
		break
	case "region":
		cache = &db.region2idMap
		break
	case "city":
		cache = &db.city2idMap
		break
	case "platform":
		cache = &db.platform2idMap
		break
	case "version":
		cache = &db.version2idMap
		break
	case "contact":
		cache = &db.contact2idMap
		break
	case "exitp":
		cache = &db.exitPol2idMap
		break
	case "exitps":
		cache = &db.exitPolSum2idMap
		break
	case "exitps6":
		cache = &db.exitPolV6Sum2idMap
		break
	default:
		panic("value2id: Invalid key/value type: " + valueType)
		break
	}

	if id, ok = (*cache)[value]; ok {
		ifPrintln(6, fmt.Sprintf("value2id: Cache hit for %s %s, returning %s.", valueType, value, id))
	} else {
		if valueType != "country" {
			id = db.addKeyValue(valueType, value)
			ifPrintln(4, fmt.Sprintf("value2id: Cache miss for %s %s, added to DB, returning %s.", valueType, value, id))
		} else {
			id = ""
			ifPrintln(4, fmt.Sprintf("value2id: Cache miss on country ID %s, returning %s.", value, id))
		}
	}
	ifPrintln(4, "func value2id: RETURN id: "+id+"\n")
	return id
}

//***************************************************************************
// Utility functions

func (db *DB) normalizeCountryID(cid string, cname string) string {
	// This is a VERY SPECIAL case
	// We do not need to lookup the country code as we already have it from
	// the Consensus and we just need to ensure it's lower case
	// However, we also need to ensure the country code exists in the Countries
	// table, and if not add it
	countryid := ""
	if len(cid) > 0 { // Country code is not empty
		countryid = strings.ToLower(cid)
		if len(db.value2id("country", countryid)) == 0 { // No country code match in DB, add it
			db.addKeyValue_CC(cid, cname) // This will also update the cache
		}
	} // else countryid will be ""
	return countryid
}

func (db *DB) addslashes(str string) string {
	// Backslash "escape" single quote, double quote, backslash, NULL
	// Keep "\" as the first in the escape sequence
	escape_chars := []string{"\\", "\"", "'", "\x00"}
	for _, c := range escape_chars {
		str = strings.Replace(str, c, "\\"+c, -1)
	}
	return str
}

func (db *DB) escapePercentSign(str string) string {
	// Backslash "escape" single quote, double quote, backslash, NULL
	// Keep "\" as the first in the escape sequence
	escape_chars := []string{"%"}
	for _, c := range escape_chars {
		str = strings.Replace(str, c, "%"+c, -1)
	}
	return str
}

// TOR Query plugin functions
func (db *DB) getTorRelaysByIDStringList(idList string) map[string](map[string]string) { // cdts generally is g_consensusDLTS
	ifPrintln(3, "func getTorRelaysByIDStringList: "+idList)
	defer ifPrintln(3, "getTorRelaysByIDStringList: END")

	if !db.initialized {
		log.Fatal("Call to a method in uninitialized database initializeLatestRelayDataCache.")
	}

	lrd := db.SQLQueryTYPEOfMaps("mapOfMaps",
		`SELECT tr.ID ID, Fingerprint, Nickname, DATE_FORMAT( First_seen, "%Y-%m-%d") as First_seen, 
		DATE_FORMAT( RecordTimeInserted, "%Y-%m-%d") as RecordTimeInserted, 
		DATE_FORMAT( RecordLastSeen, "%Y-%m-%d") as RecordLastSeen, 
		ID_Countries Country, RegionName, CityName, PlatformName, VersionName, ContactName, 
		DATE_FORMAT( Last_changed_address_or_port, "%Y-%m-%d") as Last_changed_address_or_port, 
		ExitPolicy, ExitPolicySummary, ExitPolicyV6Summary, jsd
		FROM TorRelays tr
		LEFT JOIN NodeFingerprints nf ON tr.ID_NodeFingerprints = nf.ID 
		LEFT JOIN Regions r ON tr.ID_Regions = r.ID
		LEFT JOIN Cities c ON ID_Cities = c.ID
		LEFT JOIN Platforms p ON ID_Platforms = p.ID
		LEFT JOIN Versions v ON ID_Versions = v.ID
		LEFT JOIN Contacts ct ON ID_Contacts = ct.ID
		LEFT JOIN ExitPolicies ep ON ID_ExitPolicies = ep.ID
		LEFT JOIN ExitPolicySummaries eps ON ID_ExitPolicySummaries = eps.ID
		LEFT JOIN ExitPolicyV6Summaries eps6 ON ID_ExitPolicyV6Summaries = eps6.ID 
		WHERE tr.ID IN (`+idList+`);`).(map[string](map[string]string))
	return lrd
}

func (db *DB) getLatestTRsIDsByCountryCode(cc string) map[string]string {
	ifPrintln(3, "func getLatestTRsIDsByCountryCode: "+cc)
	defer ifPrintln(3, "func getLatestTRsIDsByCountryCode: END")

	result := make(map[string]string)
	matched, err := regexp.MatchString(`^[a-z][a-z]$`, cc)
	if !matched || err != nil {
		return result
	}
	query := fmt.Sprintf("SELECT ID, ID_NodeFingerprints FROM TorRelays WHERE (ID_NodeFingerprints, RecordLastSeen) IN (SELECT ID_NodeFingerprints, max(RecordLastSeen) FROM TorRelays WHERE ID_Countries='%s' GROUP BY ID_NodeFingerprints)", cc)
	result = db.SQLQueryKeyValue(query)
	return result
}

func (db *DB) getLatestTRsIDsByEmail(email string) map[string]string {
	ifPrintln(3, "func getLatestTRsIDsByEmail: "+email)
	defer ifPrintln(3, "func getLatestTRsIDsByEmail: END")

	result := make(map[string]string)
	email = db.addslashes(email)
	query := fmt.Sprintf(`SELECT max(tr.ID) as ID, tr.ID_NodeFingerprints as ID_NodeFingerprints FROM Contacts c LEFT JOIN TorRelays tr ON c.ID = tr.ID_Contacts WHERE ContactName like '%%%s%%' GROUP BY tr.ID_NodeFingerprints;`, email)
	result = db.SQLQueryKeyValue(query)
	return result
}

func (db *DB) getLatestTRsIDsByIP(ip string) map[string]string {
	ifPrintln(3, "func getLatestTRsIDsByIP: "+ip)
	defer ifPrintln(3, "func getLatestTRsIDsByIP: END")
	result := make(map[string]string) // return value

	// Validate IP
	checkIP := net.ParseIP(ip)
	if checkIP == nil {
		return result
	}
	ip = checkIP.String()

	query := fmt.Sprintf("SELECT ID, ID_NodeFingerprints FROM TorRelays WHERE (ID_NodeFingerprints, RecordLastSeen) IN (SELECT v4.ID_NodeFingerprints, max(tr.RecordLastSeen) as RecordLastSeen FROM Exit_addresses_v4 v4 LEFT JOIN TorRelays tr on v4.ID_NodeFingerprints = tr.ID_NodeFingerprints  WHERE v4.ip4 = INET_ATON(\"%s\") GROUP BY ID_NodeFingerprints);", ip)
	result = db.SQLQueryKeyValue(query)
	return result
}
