/*****************************************************************************************
** TOR History                                                                          **
** (C) Krassimir Tzvetanov                                                              **
** Distributed under Attribution-NonCommercial-ShareAlike 4.0 International             **
** https://creativecommons.org/licenses/by-nc-sa/4.0/legalcode                          **
*****************************************************************************************/

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/yaml.v2"
)

type TorResponse struct {
	Version                      string        // required; Onionoo protocol version string.
	Next_major_version_scheduled string        // optional; UTC date (YYYY-MM-DD) when the next major protocol version is scheduled to be deployed. Omitted if no major protocol changes are planned.
	Build_revision               string        // optional # Git revision of the Onionoo instance's software used to write this response, which will be omitted if unknown.
	Relays_published             string        // required # UTC timestamp (YYYY-MM-DD hh:mm:ss) when the last known relay network status consensus started being valid. Indicates how recent the relay objects in this document are.
	Relays_skipped               uint64        // optional # Number of skipped relays as requested by a positive "offset" parameter value. Omitted if zero.
	Relays                       []TorDetails  // Relays array of objects // required # Array of relay objects as specified below.
	Relays_truncated             uint64        // optional # Number of truncated relays as requested by a positive "limit" parameter value. Omitted if zero.
	Bridges_published            string        // required # UTC timestamp (YYYY-MM-DD hh:mm:ss) when the last known bridge network status was published. Indicates how recent the bridge objects in this document are.
	Bridges_skipped              uint64        // optional # Number of skipped bridges as requested by a positive "offset" parameter value. Omitted if zero.
	Bridges                      []interface{} // Bridges array of objects // required # Array of bridge objects as specified below.
	Bridges_truncated            uint64        // optional # Number of truncated bridges as requested by a positive "limit" parameter value. Omitted if zero.
}

type TorDetails struct {
	Nickname                     string      `json:",omitempty"` // required # Relay nickname consisting of 1–19 alphanumerical characters. Turned into required field on March 14, 2018.
	Fingerprint                  string      `json:",omitempty"` // required # Relay fingerprint consisting of 40 upper-case hexadecimal characters.
	Or_addresses                 []string    `json:",omitempty"` // required # Array of IPv4 or IPv6 addresses and TCP ports or port lists where the relay accepts onion-routing connections. The first address is the primary onion-routing address that the relay used to register in the network, subsequent addresses are in arbitrary order. IPv6 hex characters are all lower-case.
	Exit_addresses               []string    `json:",omitempty"` // optional # Array of IPv4 addresses that the relay used to exit to the Internet in the past 24 hours. Omitted if array is empty. Changed on April 17, 2018 to include all exit addresses, regardless of whether they are used as onion-routing addresses or not.
	Dir_address                  string      `json:",omitempty"` // optional # IPv4 address and TCP port where the relay accepts directory connections. Omitted if the relay does not accept directory connections.
	Last_seen                    string      `json:",omitempty"` // required # UTC timestamp (YYYY-MM-DD hh:mm:ss) when this relay was last seen in a network status consensus.
	Last_changed_address_or_port string      `json:",omitempty"` // required # UTC timestamp (YYYY-MM-DD hh:mm:ss) when this relay last stopped announcing an IPv4 or IPv6 address or TCP port where it previously accepted onion-routing or directory connections. This timestamp can serve as indicator whether this relay would be a suitable fallback directory.
	First_seen                   string      `json:",omitempty"` // required # UTC timestamp (YYYY-MM-DD hh:mm:ss) when this relay was first seen in a network status consensus.
	Running                      bool        `json:",omitempty"` // required # Boolean field saying whether this relay was listed as running in the last relay network status consensus.
	Hibernating                  bool        `json:",omitempty"` // optional # Boolean field saying whether this relay indicated that it is hibernating in its last known server descriptor. This information may be helpful to decide whether a relay that is not running anymore has reached its accounting limit and has not dropped out of the network for another, unknown reason. Omitted if either the relay is not hibernating, or if no information is available about the hibernation status of the relay.
	Flags                        []string    `json:",omitempty"` // optional # Array of relay flags that the directory authorities assigned to this relay. May be omitted if empty.
	Country                      string      `json:",omitempty"` // optional # Two-letter lower-case country code as found in a flagsIP database by resolving the relay's first onion-routing IP address. Omitted if the relay IP address could not be found in the GeoIP database.
	Country_name                 string      `json:",omitempty"` // optional # Country name as found in a GeoIP database by resolving the relay's first onion-routing IP address. Omitted if the relay IP address could not be found in the GeoIP database, or if the GeoIP database did not contain a country name.
	Region_name                  string      `json:",omitempty"` // optional # Region name as found in a GeoIP database by resolving the relay's first onion-routing IP address. Omitted if the relay IP address could not be found in the GeoIP database, or if the GeoIP database did not contain a region name.
	City_name                    string      `json:",omitempty"` // optional # City name as found in a GeoIP database by resolving the relay's first onion-routing IP address. Omitted if the relay IP address could not be found in the GeoIP database, or if the GeoIP database did not contain a city name.
	Latitude                     float64     `json:",omitempty"` // optional # Latitude as found in a GeoIP database by resolving the relay's first onion-routing IP address. Omitted if the relay IP address could not be found in the GeoIP database.
	Longitude                    float64     `json:",omitempty"` // optional # Longitude as found in a GeoIP database by resolving the relay's first onion-routing IP address. Omitted if the relay IP address could not be found in the GeoIP database.
	As                           string      `json:",omitempty"` // optional # AS number as found in an AS database by resolving the relay's first onion-routing IP address. AS number strings start with "AS", followed directly by the AS number. Omitted if the relay IP address could not be found in the AS database. Added on August 3, 2018.
	As_number                    string      `json:",omitempty"` // OBSOLETE optional # AS number as found in an AS database by resolving the relay's first onion-routing IP address. AS number strings start with "AS", followed directly by the AS number. Omitted if the relay IP address could not be found in the AS database. Removed on September 10, 2018.
	As_name                      string      `json:",omitempty"` // optional # AS name as found in an AS database by resolving the relay's first onion-routing IP address. Omitted if the relay IP address could not be found in the AS database.
	Consensus_weight             uint64      `json:",omitempty"` // required # Weight assigned to this relay by the directory authorities that clients use in their path selection algorithm. The unit is arbitrary; currently it's kilobytes per second, but that might change in the future.
	Host_name                    string      `json:",omitempty"` // optional # Host name as found in a reverse DNS lookup of the relay's primary IP address. This field is updated at most once in 12 hours, unless the relay IP address changes. Omitted if the relay IP address was not looked up, if no lookup request was successful yet, or if no A record was found matching the PTR record. Deprecated on July 16, 2018.
	Verified_host_names          []string    `json:",omitempty"` // optional # Host names as found in a reverse DNS lookup of the relay's primary IP address for which a matching A record was also found. This field is updated at most once in 12 hours, unless the relay IP address changes. Omitted if the relay IP address was not looked up, if no lookup request was successful yet, or if no A records were found matching the PTR records (i.e. it was not possible to verify the value of any of the PTR records). A DNSSEC validating resolver is used for these lookups. Failure to validate DNSSEC signatures will prevent those names from appearing in this field. Added on July 16, 2018. Updated to clarify that a DNSSEC validating resolver is used on August 17, 2018.
	Unverified_host_names        []string    `json:",omitempty"` // optional # Host names as found in a reverse DNS lookup of the relay's primary IP address that for which a matching A record was not found. This field is updated at most once in 12 hours, unless the relay IP address changes. Omitted if the relay IP address was not looked up, if no lookup request was successful yet, or if A records were found matching all PTR records (i.e. it was possible to verify the value of each of the PTR records). A DNSSEC validating resolver is used for these lookups. Failure to validate DNSSEC signatures will prevent those names from appearing in this field. Added on July 16, 2018. Updated to clarify that a DNSSEC validating resolver is used on August 17, 2018.
	Last_restarted               string      `json:",omitempty"` // optional # UTC timestamp (YYYY-MM-DD hh:mm:ss) when the relay was last (re-)started. Missing if router descriptor containing this information cannot be found.
	Bandwidth_rate               uint64      `json:",omitempty"` // optional # Average bandwidth in bytes per second that this relay is willing to sustain over long periods. Missing if router descriptor containing this information cannot be found.
	Bandwidth_burst              uint64      `json:",omitempty"` // optional # Bandwidth in bytes per second that this relay is willing to sustain in very short intervals. Missing if router descriptor containing this information cannot be found.
	Observed_bandwidth           uint64      `json:",omitempty"` // optional # Bandwidth estimate in bytes per second of the capacity this relay can handle. The relay remembers the maximum bandwidth sustained output over any ten second period in the past day, and another sustained input. The "observed_bandwidth" value is the lesser of these two numbers. Missing if router descriptor containing this information cannot be found.
	Advertised_bandwidth         uint64      `json:",omitempty"` // optional # Bandwidth in bytes per second that this relay is willing and capable to provide. This bandwidth value is the minimum of bandwidth_rate, bandwidth_burst, and observed_bandwidth. Missing if router descriptor containing this information cannot be found.
	Exit_policy                  []string    `json:",omitempty"` // optional # Array of exit-policy lines. Missing if router descriptor containing this information cannot be found. May contradict the "exit_policy_summary" field in a rare edge case: this happens when the relay changes its exit policy after the directory authorities summarized the previous exit policy.
	Exit_policy_summary          interface{} `json:",omitempty"` // optional # Summary version of the relay's exit policy containing a dictionary with either an "accept" or a "reject" element. If there is an "accept" ("reject") element, the relay accepts (rejects) all TCP ports or port ranges in the given list for most IP addresses and rejects (accepts) all other ports. May contradict the "exit_policy" field in a rare edge case: this happens when the relay changes its exit policy after the directory authorities summarized the previous exit policy.
	Exit_policy_v6_summary       interface{} `json:",omitempty"` // optional # Summary version of the relay's IPv6 exit policy containing a dictionary with either an "accept" or a "reject" element. If there is an "accept" ("reject") element, the relay accepts (rejects) all TCP ports or port ranges in the given list for most IP addresses and rejects (accepts) all other ports. Missing if the relay rejects all connections to IPv6 addresses. May contradict the "exit_policy_summary" field in a rare edge case: this happens when the relay changes its exit policy after the directory authorities summarized the previous exit policy.
	Contact                      string      `json:",omitempty"` // optional # Contact address of the relay operator. Omitted if empty or if descriptor containing this information cannot be found.
	Platform                     string      `json:",omitempty"` // optional # Platform string containing operating system and Tor version details. Omitted if empty or if descriptor containing this information cannot be found.
	Version                      string      `json:",omitempty"` // optional # Tor software version without leading "Tor" as reported by the directory authorities in the "v" line of the consensus. Omitted if either the directory authorities or the relay did not report which version the relay runs or if the relay runs an alternative Tor implementation.
	Recommended_version          bool        `json:",omitempty"` // optional # Boolean field saying whether the Tor software version of this relay is recommended by the directory authorities or not. Uses the relay version in the consensus. Omitted if either the directory authorities did not recommend versions, or the relay did not report which version it runs.
	Version_status               string      `json:",omitempty"` // optional # Status of the Tor software version of this relay based on the versions recommended by the directory authorities. Possible version statuses are: "recommended" if a version is listed as recommended; "experimental" if a version is newer than every recommended version; "obsolete" if a version is older than every recommended version; "new in series" if a version has other recommended versions with the same first three components, and the version is newer than all such recommended versions, but it is not newer than every recommended version; "unrecommended" if none of the above conditions hold. Omitted if either the directory authorities did not recommend versions, or the relay did not report which version it runs. Added on April 6, 2018.
	Effective_family             []string    `json:",omitempty"` // optional # Array of fingerprints of relays that are in an effective, mutual family relationship with this relay. These relays are part of this relay's family and they consider this relay to be part of their family. Always contains the relay's own fingerprint. Omitted if the descriptor containing this information cannot be found. Updated to always include the relay's own fingerprint on March 14, 2018.
	Alleged_family               []string    `json:",omitempty"` // optional # Array of fingerprints of relays that are not in an effective, mutual family relationship with this relay. These relays are part of this relay's family but they don't consider this relay to be part of their family. Omitted if empty or if descriptor containing this information cannot be found.
	Indirect_family              []string    `json:",omitempty"` // optional # Array of fingerprints of relays that are not in an effective, mutual family relationship with this relay but that can be reached by following effective, mutual family relationships starting at this relay. Omitted if empty or if descriptor containing this information cannot be found.
	Consensus_weight_fraction    float64     `json:",omitempty"` // optional # Fraction of this relay's consensus weight compared to the sum of all consensus weights in the network. This fraction is a very rough approximation of the probability of this relay to be selected by clients. Omitted if the relay is not running.
	Guard_probability            float64     `json:",omitempty"` // optional # Probability of this relay to be selected for the guard position. This probability is calculated based on consensus weights, relay flags, and bandwidth weights in the consensus. Path selection depends on more factors, so that this probability can only be an approximation. Omitted if the relay is not running, or the consensus does not contain bandwidth weights.
	Middle_probability           float64     `json:",omitempty"` // optional # Probability of this relay to be selected for the middle position. This probability is calculated based on consensus weights, relay flags, and bandwidth weights in the consensus. Path selection depends on more factors, so that this probability can only be an approximation. Omitted if the relay is not running, or the consensus does not contain bandwidth weights.
	Exit_probability             float64     `json:",omitempty"` // optional # Probability of this relay to be selected for the exit position. This probability is calculated based on consensus weights, relay flags, and bandwidth weights in the consensus. Path selection depends on more factors, so that this probability can only be an approximation. Omitted if the relay is not running, or the consensus does not contain bandwidth weights.
	Measured                     bool        `json:",omitempty"` // optional # Boolean field saying whether the consensus weight of this relay is based on a threshold of 3 or more measurements by Tor bandwidth authorities. Omitted if the network status consensus containing this relay does not contain measurement information.
	Unreachable_or_addresses     []string    `json:",omitempty"` // optional # Array of IPv4 or IPv6 addresses and TCP ports or port lists where the relay claims in its descriptor to accept onion-routing connections but that the directory authorities failed to confirm as reachable. Contains only additional addresses of a relay that are found unreachable and only as long as a minority of directory authorities performs reachability tests on these additional addresses. Relays with an unreachable primary address are not included in the network status consensus and excluded entirely. Likewise, relays with unreachable additional addresses tested by a majority of directory authorities are not included in the network status consensus and excluded here, too. If at any point network status votes will be added to the processing, relays with unreachable addresses will be included here. Addresses are in arbitrary order. IPv6 hex characters are all lower-case. Omitted if empty.
}

type TorHistoryConfig struct {
	DBServer struct {
		Port     string `yaml:"port"`
		Host     string `yaml:"host"`
		DBName   string `yaml:"database"`
		Username string `yaml:"username"`
		Password string `yaml:"password"`
	} `yaml:"dbserver"`
	Tor struct {
		URL string `yaml:"url"`
	} `yaml:"consensus"`
}

var g_config TorHistoryConfig

var g_consensus_details_URL = "https://onionoo.torproject.org/details"
var g_db *DB
var g_consensusDLTS string

var f_inputFileName, f_consensusBackupFileName *string
var f_configFileName *string
var f_nodeFilter *string
var f_nodeInfo *bool
var f_expandIPs, f_expandIPsAndFlags *bool
var f_debug *bool
var f_verbosity *uint
var f_consensusDownloadTime, f_fmtConsensusDownloadTime *string

func ifPrintln(level int, msg string) {
	// Prints an error message if verbosity level is less than f_verbosity threshold
	if uint(math.Abs(float64(level))) < *f_verbosity {
		if level < 0 {
			fmt.Fprintf(os.Stderr, msg+"\n")
		} else {
			fmt.Fprintf(os.Stdout, msg+"\n")
		}
	}
}

func getConsensusDLTimestamp(cmdlineTS string) string {
	var t time.Time
	// Consensus download time override
	if len(cmdlineTS) > 0 {
		ifPrintln(-2, "consensusDownloadTime: processing command line arguments")
		var formats []string
		// Time format override
		if len(*f_fmtConsensusDownloadTime) > 0 {
			ifPrintln(4, "CUSTOM FORMAT: "+*f_fmtConsensusDownloadTime)
			formats = append(formats, *f_fmtConsensusDownloadTime)
		} else {
			formats = []string{"2006-01-02_15:04:05", "20060102150405", time.RFC3339,
				time.RFC3339Nano, time.ANSIC, time.UnixDate, time.RFC822, time.RFC822Z,
				time.RFC850, time.RFC1123, time.RFC1123Z, time.RubyDate}
		}
		var err error
		for _, f := range formats {
			ifPrintln(6, "Attempting format: "+f)
			t, err = time.Parse(f, cmdlineTS)
			if err == nil {
				break
			}
		}
	} else {
		ifPrintln(-2, "consensusDownloadTime: using system time")
		t = time.Now()
		fmt.Println("System time:")
	}
	str := fmt.Sprintf("%04d%02d%02d%02d%02d%02d",
		t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second())
	ifPrintln(2, "consensusDownloadTime: returning timestamp: "+str)
	return str
}

func initialize() {
	// Parse command line arguments first, to find the config file path
	parseCmdlnArguments()

	// Read config file if one specified
	parseConfigFile(f_configFileName, &g_config)

	// Acquire the Consensus download time. If importing from a file, it is
	// taken from the command line. If downloaded it's now()
	g_consensusDLTS = getConsensusDLTimestamp(*f_consensusDownloadTime)

	// Open DB connection
	g_db = NewDBFromConfig(g_config)

	// Initialize DB caches
	g_db.initCaches()

	// Initialize CC cache
	g_db.initCountryNameCache()

	ifPrintln(2, "Caches initialized.")
}

func cleanup() {
	ifPrintln(5, "Starting cleanup()")
	g_db.Close()
	ifPrintln(5, "Completed cleanup()")
}

func readConsensusDataFromFile(fn string) *json.Decoder {
	consensusDataFile, err := os.Open(fn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: opening Consensus data file (%s). ", err.Error())
		log.Fatal(err)
	}
	return json.NewDecoder(consensusDataFile)
}

func main() {
	initialize()
	defer cleanup()

	// Extract the TOR node filters from the arguments
	//matchFlags := parseNodeFilters(f_nodeFilter)

	var consensusData *json.Decoder
	if *f_inputFileName != "" {
		consensusData = readConsensusDataFromFile(*f_inputFileName)
	} else {
		consensusData = downloadConsensus(g_consensus_details_URL)
	}

	var tor_response TorResponse
	err := consensusData.Decode(&tor_response)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parsing Consensus file: %s", err.Error())
		log.Fatal(err)
	}

	ifPrintln(-3, fmt.Sprintf("TOR Version, build revision: %s, %s", tor_response.Version, tor_response.Build_revision))
	g_db.addToTorQueries(tor_response.Version, tor_response.Relays_published, tor_response.Bridges_published)

	// #### Update the SQL query at top whe done here
	g_db.stmtAddTorRelays, err = g_db.dbh.Prepare("INSERT INTO TorRelays (ID_NodeFingerprints, ID_Countries, ID_Regions, ID_Cities, ID_Platforms, ID_Versions, ID_Contacts, " +
		"ID_ExitPolicies, ID_ExitPolicySummaries, ID_ExitPolicyV6Summaries, Nickname, Last_changed_address_or_port, First_seen, flags, jsd) " +
		"VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);")
	//###### UPDATE this query up

	var lrd map[string](map[string]string) // lrd = latest relay data
	lrd = g_db.SQLQueryTYPEOfMaps("mapOfMaps",
		`SELECT Fingerprint, tr.ID id, Nickname, RecordTimeInserted, DATE_FORMAT( RecordLastSeen, "%Y%m%d%H%i%s") as RecordLastSeen, ID_Countries Country, CityName, 
			PlatformName, VersionName, ContactName, First_seen, Last_changed_address_or_port, ExitPolicy, ExitPolicySummary, ExitPolicyV6Summary , ID_Versions, ID_Contacts, ID_NodeFingerprints
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
			(SELECT ID_NodeFingerprints, max(RecordLastSeen) FROM TorRelays GROUP BY ID_NodeFingerprints);`).(map[string](map[string]string))
	// ### BIG BUG => RecordTimeInserted should be RecordLastSeen ^^^
	// #####

	for _, relay := range tor_response.Relays {
		ifPrintln(2, "\n== "+relay.Fingerprint+" ==========================================")
		// Check if this is a newer record
		// Prepare JSON values for comparisson
		js_exitp, _ := json.Marshal(relay.Exit_policy)
		js_exitps, _ := json.Marshal(relay.Exit_policy_summary)
		js_exitps6, _ := json.Marshal(relay.Exit_policy_v6_summary)

		// Clean up excess space left/right
		relay.Contact = strings.TrimSpace(relay.Contact)

		// The check below needs to be segmented so subtables can be updated independently of TorRelays
		fp := relay.Fingerprint
		ifPrintln(6, "Comparing records for fingerprint: "+fp)
		if recordsMatch(relay, lrd[fp]) { // MATCH - just update RLS
			ifPrintln(2, "DEBUG: g_consensusDLTS: "+g_consensusDLTS+"; lrd[fp]['RecordLastSeen']: "+lrd[fp]["RecordLastSeen"])
			if g_consensusDLTS == lrd[fp]["RecordLastSeen"] {
				fmt.Println("DEBUG: TorRelay records TIMESTAMPS MATCH!!! No DB update need at all")
			} else {
				// Update the RecordLastSeen (RLS) timestamp
				g_db.updateTorRelayRLS(lrd[fp]["id"], g_consensusDLTS)
				// Note that if Last_changed_address_or_port has not changed, there is no need to explicitly check
				// if Or, Exit and Dir have changed, however we are goingto update their RLS to
				// speed up queries against those index tables.

				//Yes, update needed #########################
				if len(relay.Or_addresses) > 0 {
					for _, or := range relay.Or_addresses {
						//fmt.Println("TorRelay: Or_addresses: " + or)
						g_db.updateIfNeededRelayAddressRLS("Or", lrd[fp]["ID_NodeFingerprints"], g_consensusDLTS, or)
					}
				}

				if len(relay.Exit_addresses) > 0 {
					for _, ex := range relay.Exit_addresses {
						//fmt.Println("TorRelay: Exit_addresses" + ex)
						g_db.updateIfNeededRelayAddressRLS("Ex", lrd[fp]["ID_NodeFingerprints"], g_consensusDLTS, ex)

					}
				}

				if len(relay.Dir_address) > 0 {
					//fmt.Println("TorRelay: Dir_addresses" + relay.Dir_address)
					g_db.updateIfNeededRelayAddressRLS("Di", lrd[fp]["ID_NodeFingerprints"], g_consensusDLTS, relay.Dir_address)
				}
			}
			continue
		} else { // No match/New Record/Add to DB

			// If newer write to DB
			fpid := g_db.value2id("fingerprint", relay.Fingerprint)
			countryid := g_db.normalizeCountryID(relay.Country, relay.Country_name)
			regionid := g_db.value2id("region", relay.Region_name)
			cityid := g_db.value2id("city", relay.City_name)
			platformid := g_db.value2id("platform", relay.Platform)
			versionid := g_db.value2id("version", relay.Version)
			contactid := g_db.value2id("contact", relay.Contact)
			exitp := g_db.value2id("exitp", string(js_exitp))
			exitps := g_db.value2id("exitps", string(js_exitps))
			exitps6 := g_db.value2id("exitps6", string(js_exitps6))

			// Store in intermediate variables before compacting the JSON object (before it's stored)
			nick := relay.Nickname
			lastChanged := relay.Last_changed_address_or_port
			firstSeen := relay.First_seen

			// Cleanup the JSON object before marshaling
			cleanupRelayStruct(&relay)

			// ####
			jsFlags, _ := json.Marshal(relay.Flags)
			/*fmt.Println("=====Flags ==========")
			fmt.Println(relay.Flags)
			fmt.Println("==== js Flags ===========")
			fmt.Println(jsFlags)
			fmt.Println("===============")
			*/
			jsRelay, _ := json.Marshal(relay)

			//			fmt.Printf("=== DEBUG ========================================\n"+
			//				"fpid: %s\ncountryid: %s\nregionid: %s\ncityid: %s\nplatformid: %s\nversionid: %s\ncontactid: %s\nrelay.Nickname: %s\nrelay.Last_changed_address_or_port: %s\nrelay.First_seen: %s\njsFlags: %s\njsRelay: %s\n",
			//				fpid, countryid, regionid, cityid, platformid, versionid, contactid, nick, lastChanged, firstSeen, jsFlags, jsRelay)

			res, err := g_db.stmtAddTorRelays.Exec(fpid, countryid, regionid, cityid, platformid, versionid, contactid,
				exitp, exitps, exitps6, nick, lastChanged, firstSeen, jsFlags, jsRelay)
			if err != nil {
				fmt.Printf("fpid: %s\ncountryid: %s\nregionid: %s\ncityid: %s\nrelay.Nickname: %s\n"+
					"relay.Last_changed_address_or_port: %s\nrelay.First_seen: %s\njsFlags: %s\njsRelay: %s\n",
					fpid, countryid, regionid, cityid, nick, lastChanged, firstSeen, jsFlags, jsRelay)
				panic("func main: g_db.stmtAddTorRelays.Exec: " + err.Error())
			}

			lastID_int64, err := res.LastInsertId()
			lastID := fmt.Sprintf("%d", lastID_int64)
			fmt.Println("TorRelay LastInsertID: " + lastID)

			fmt.Println("TorRelay: Loop Or_addresses")
			fmt.Println(relay.Or_addresses)
			if len(relay.Or_addresses) > 0 {
				for _, or := range relay.Or_addresses {
					fmt.Println("TorRelay: Or_addresses" + or)
					g_db.addToIP("Or", fpid, g_consensusDLTS, g_consensusDLTS, or)
				}
			}

			fmt.Println("TorRelay: Loop Exit_addresses")
			fmt.Println(relay.Exit_addresses)
			if len(relay.Exit_addresses) > 0 {
				for _, ex := range relay.Exit_addresses {
					fmt.Println("TorRelay: Exit_addresses" + ex)
					g_db.addToIP("Exit", fpid, g_consensusDLTS, g_consensusDLTS, ex)
				}
			}

			// relay.Dir_address is a string not an array
			fmt.Println("TorRelay: Dir_addresses" + relay.Dir_address)
			if len(relay.Dir_address) > 0 {
				g_db.addToIP("Dir", fpid, g_consensusDLTS, g_consensusDLTS, relay.Dir_address)
			}

			/*
				if allStringsInSet(&matchFlags, &relay.Flags) {

					if *f_nodeInfo {
						fmt.Printf("NODE: %s/%s/%s (%s) => %s\n", relay.Nickname, relay.Fingerprint, fpid, relay.Flags, relay.Exit_addresses)
					}
					if *f_expandIPs {
						for _, i := range relay.Exit_addresses {
							fmt.Println(i)
						}
					}
					if *f_expandIPsAndFlags {
						for _, i := range relay.Exit_addresses {
							fmt.Printf("%s: %s\n", i, relay.Flags)
						}
					}
				}
			*/
		}
	}
	ifPrintln(5, "DONE: parsing Consensus file")
}

func recordsMatch(relay TorDetails, lrdfp map[string]string) bool {
	// Prepare the JSON objects
	js_exitp, _ := json.Marshal(relay.Exit_policy)
	js_exitps, _ := json.Marshal(relay.Exit_policy_summary)
	js_exitps6, _ := json.Marshal(relay.Exit_policy_v6_summary)

	if relay.Nickname == lrdfp["Nickname"] &&
		relay.Country == lrdfp["Country"] &&
		relay.City_name == lrdfp["CityName"] &&
		relay.Platform == lrdfp["PlatformName"] &&
		relay.Version == lrdfp["VersionName"] &&
		strings.ToLower(relay.Contact) == strings.ToLower(lrdfp["ContactName"]) &&
		relay.Last_changed_address_or_port == lrdfp["Last_changed_address_or_port"] &&
		string(js_exitp) == lrdfp["ExitPolicy"] &&
		string(js_exitps) == lrdfp["ExitPolicySummary"] &&
		string(js_exitps6) == lrdfp["ExitPolicyV6Summary"] {

		ifPrintln(2, "MATCHED: "+lrdfp["Fingerprint"])
		return true
	} else {
		// #### Just for debugging - delete later
		ifPrintln(2, "NO MATCH: storing node: "+lrdfp["Fingerprint"])
		fmt.Printf("Nickname: %s => %s\n", relay.Nickname, lrdfp["Nickname"])
		fmt.Printf("Country: %s => %s\n", relay.Country, lrdfp["Country"])
		fmt.Printf("City Name: %s => %s\n", relay.City_name, lrdfp["CityName"])
		fmt.Printf("Platform: %s => %s\n", relay.Platform, lrdfp["PlatformName"])
		fmt.Printf("Version: %s => %s (%s)\n", relay.Version, lrdfp["VersionName"], lrdfp["ID_Versions"])
		fmt.Printf("Contact: %s => %s (%s)\n", relay.Contact, lrdfp["ContactName"], lrdfp["ID_Contacts"])
		fmt.Printf("LastCHAP: %s => %s\n", relay.Last_changed_address_or_port, lrdfp["Last_changed_address_or_port"])
		fmt.Printf("FirstSeen: %s => %s\n", relay.First_seen, lrdfp["First_seen"])
		return false
	}
}

func cleanupRelayStruct(pr *TorDetails) {
	pr.Nickname = ""
	pr.Country = ""
	pr.Country_name = ""
	pr.Region_name = ""
	pr.City_name = ""
	pr.Platform = ""
	pr.Version = ""
	pr.Contact = ""
	pr.Last_changed_address_or_port = ""
	pr.First_seen = ""
	pr.Fingerprint = ""
	pr.Exit_policy = nil
	pr.Exit_policy_summary = nil
	pr.Exit_policy_v6_summary = nil
	// Store those in the JSON for now, remove when thoroughly tested.
	//	pr.Or_addresses = ""
	//	pr.Exit_addresses = ""
	//	pr.Dir_address = ""
	// ##### Deal with soon as it is highly volotile: pr.Last_seen = ""
}

func parseConfigFile(cfgFileName *string, cfg *TorHistoryConfig) {
	if *cfgFileName == "" {
		return
	}
	f, err := os.Open(*cfgFileName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to open configuration file: %s", *cfgFileName)
		return
	}
	defer f.Close()

	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "YAML Decoder error: %s", err)
		return
	}
	ifPrintln(-9, fmt.Sprintf("Host: %s\nPort: %s\nUsername: %s\nPassword: %s\nDB Name: %s",
		cfg.DBServer.Host, cfg.DBServer.Port, cfg.DBServer.Username, cfg.DBServer.Password, cfg.DBServer.DBName))
}

/*func stringInSet( s *string, set []string) bool {
	for _, curStr := range set {
		if curStr == *s {
			return true
		}
	}
	return false
}*/

func allStringsInSet(needles *[]string, set *[]string) bool {
	//fmt.Println("allStringsInSet")
	if len(*needles) == 0 { // Optimization - if no needles - always true
		//fmt.Println("no needles")
		return true
	}
NeedleLoop:
	for _, curNeedle := range *needles {
		//found := false
		//fmt.Printf("Current Needle: %s \n", curNeedle)
		for _, curStr := range *set {
			//fmt.Printf("  comp to %s\n", curStr)
			if curStr == curNeedle {
				//fmt.Println("needle match")
				continue NeedleLoop
			}
		}
		//if !found {
		//fmt.Println("all needle fail")
		return false
		//}
	}
	//fmt.Println("all needles match")
	return true
}

func parseCmdlnArguments() {
	f_inputFileName = flag.String("input-data-file", "details.json", "Use input file instead of downloading from the consensus")
	f_configFileName = flag.String("config-file-name", "config.yml", "Full path of YAML config file")
	f_consensusBackupFileName = flag.String("consensus-backup-file", "", "Make a backup of the consensus as downloaded at the supplied destination path/prefix")
	f_nodeFilter = flag.String("node-flag", "", "Node flag filter: BadExit, Exit, Fast, Guard, HSDir, Running, Stable, StaleDesc, V2Dir and Valid")
	f_nodeInfo = flag.Bool("node-info", true, "Generic node information (on by default)")
	f_expandIPs = flag.Bool("expand-ips", false, "Forces one per line expansion of the IPs in the answer section")
	f_expandIPsAndFlags = flag.Bool("expand-ips-flags", false, "Forces one per line expansion of the IPs in the answer section")
	f_debug = flag.Bool("debug", true, "Debug (true/false)")
	f_verbosity = flag.Uint("verbosity", 3, "Verbosity level if negative print to Stderr")
	f_consensusDownloadTime = flag.String("consensus-download-time", "", "The time the consensus was downloaded. Useful when importing data downloaded in the past")
	f_fmtConsensusDownloadTime = flag.String("consensus-download-time-format", "", "The time the consensus was downloaded. Useful when importing data downloaded in the past")

	flag.Parse()

	// Disable f_expandIPs if f_expandIPsAndFlags is on
	if *f_expandIPsAndFlags {
		*f_expandIPs = false
	}

	if *f_debug && len(flag.Args()) > 0 {
		fmt.Println(os.Stderr, "DEBUG: Unprocessed args:", flag.Args())
	}
}

func parseNodeFilters(f_nodeFilter *string) []string {
	var matchFlags []string
	if *f_nodeFilter == "" {
		if *f_debug {
			fmt.Fprintln(os.Stderr, "No filters were applied")
		}
	} else {
		matchFlags = strings.Split(*f_nodeFilter, ",")
		if *f_debug {
			fmt.Fprintf(os.Stderr, "DEBUG: nodeFlag(s) in filter: ")
			for _, i := range matchFlags {
				fmt.Fprintf(os.Stderr, " %s", i)
			}
			fmt.Fprintf(os.Stderr, "\n")
		}
	}
	return matchFlags
}

func downloadConsensus(url string) *json.Decoder {
	ifPrintln(3, "Downloading Consensus details from: "+url)

	http_session, err := http.Get(url)
	if err != nil {
		log.Fatal(err)
	}
	defer http_session.Body.Close()

	// Check if backup is requested
	if *f_consensusBackupFileName == "" {
		ifPrintln(-5, "No backup requested.")
		return json.NewDecoder(http_session.Body)
	} else {
		ifPrintln(-5, "Backup requested.")

		data, err := ioutil.ReadAll(http_session.Body)
		if err != nil {
			log.Fatal(err)
		}

		t := time.Now().UTC()
		backup_file, _ := os.Create(*f_consensusBackupFileName + "-" + t.Format("20060102150405"))
		defer backup_file.Close()

		// Write to backup
		backup_file.Write(data)

		// Write to parser
		return json.NewDecoder(bytes.NewReader(data))
	}
}
