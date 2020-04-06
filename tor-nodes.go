/*****************************************************************************************
** TOR History                                                                          **
** (C) Krassimir Tzvetanov                                                              **
** Distributed under Attribution-NonCommercial-ShareAlike 4.0 International             **
** https://creativecommons.org/licenses/by-nc-sa/4.0/legalcode                          **
*****************************************************************************************/

package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"regexp"
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
	Verbosity uint `yaml:"verbosity"`
	Quiet     bool // Overrides and level of verbosity; cannot be configured in config file
	//CfgFilename string
	DBServer struct {
		Enabled  bool   //`yaml:"enabled"`
		Port     string `yaml:"port"`
		Host     string `yaml:"host"`
		DBName   string `yaml:"database"`
		Username string `yaml:"username"`
		Password string `yaml:"password"`
	} `yaml:"dbserver"`
	Tor struct {
		ConsensusURL     string `yaml:"url"`      // Consensus URL
		Filename         string `yaml:"Filename"` // Input filename
		ConsensusDLT     string
		ConsensusDLT_fmt string

		ExtractDLTfromFilename       bool
		ExtractDLTfromFilename_regex string
	} `yaml:"consensus"`
	Backup struct {
		Filename string `yaml:"filename"`
		Gzip     bool   `yaml:"gzip"`
	} `yaml:"backup"`
}

var g_config TorHistoryConfig
var g_consensus_details_URL = "https://onionoo.torproject.org/details"
var g_db *DB

var g_consensusDLTS string

var f_nodeFilter *string
var f_nodeInfo *bool
var f_expandIPs, f_expandIPsAndFlags *bool

// Prints an error message if verbosity level is less than g_config.Verbosity threshold
// Observes "Quiet" and suppresses all verbosity
func ifPrintln(level int, msg string) {
	if g_config.Quiet && level > 0 { // stderr (level<0) is exempt from quiet
		return
	}
	if uint(math.Abs(float64(level))) <= g_config.Verbosity {
		if level < 0 {
			fmt.Fprintf(os.Stderr, msg+"\n")
		} else {
			fmt.Fprintf(os.Stdout, msg+"\n")
		}
	}
}

func getConsensusDLTimestamp() string { // cmdlineTS string
	ifPrintln(6, "getConsensusDLTimestamp: ")
	if g_config.Tor.ExtractDLTfromFilename && len(g_config.Tor.ConsensusDLT) > 0 {
		log.Fatalln("Incompatible flags extract-consensus-download-time-from-filename and consensus-download-time. Remove one of them.")
	}

	var t time.Time

	if !g_config.Tor.ExtractDLTfromFilename && len(g_config.Tor.ConsensusDLT) == 0 {
		ifPrintln(-3, "consensusDownloadTime: using system time")
		t = time.Now()
	} else {
		ifPrintln(-3, "consensusDownloadTime: not using system time, processing command line arguments")
		// Consensus download time override
		var ts_matches []string
		if len(g_config.Tor.ConsensusDLT) > 0 {
			ts_matches = make([]string, 1)
			ts_matches[0] = g_config.Tor.ConsensusDLT
		}
		if g_config.Tor.ExtractDLTfromFilename {
			// If RegEx supplied - try it
			if len(g_config.Tor.ExtractDLTfromFilename_regex) > 0 { // If RegEx is provided use it to extract the date
				re := regexp.MustCompile(g_config.Tor.ExtractDLTfromFilename_regex)
				ts_matches = re.FindAllString(g_config.Tor.Filename, -1)
			} else { //No regex, try the old way
				re := regexp.MustCompile(`[0-9][0-9-_:]+[0-9]`)
				ts_matches = re.FindAllString(g_config.Tor.Filename, -1)
			}
			ifPrintln(6, fmt.Sprintf("Extracted timestamp from filename: \n%v", ts_matches))
		}
		formats := getTimeFormats()
		t_res := matchTimestampToFormats(ts_matches, formats)
		if t_res == nil {
			log.Fatalln("Unable to parse timestamp.", ts_matches)
		}
		t = *t_res
	} // else

	str := fmt.Sprintf("%04d%02d%02d%02d%02d%02d",
		t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second())
	ifPrintln(3, "consensusDownloadTime: returning timestamp: "+str)
	return str
}

func getTimeFormats() []string {
	var formats []string
	// Time format override
	if len(g_config.Tor.ConsensusDLT_fmt) > 0 {
		ifPrintln(4, "Custom format supplied: "+g_config.Tor.ConsensusDLT_fmt)
		formats = append(formats, g_config.Tor.ConsensusDLT_fmt)
	} else {
		formats = []string{"2006-01-02_15:04:05", "2006-01-02_15:04", "20060102150405", "200601021504",
			"2006-01-02-15-04-05", "2006-01-02-15-04", time.RFC3339, time.RFC3339Nano, time.ANSIC, time.UnixDate,
			time.RFC822, time.RFC822Z, time.RFC850, time.RFC1123, time.RFC1123Z, time.RubyDate}
	}
	return formats
}

func matchTimestampToFormats(ts_matches []string, formats []string) *time.Time {
	// Given an array of potential timestamps and possible time formats it returns
	// a match for the first TS that matches a time format
	var err error
	var t time.Time

	for _, ts := range ts_matches {
		ifPrintln(6, "Matching against: "+ts)
		for _, f := range formats {
			ifPrintln(6, "Attempting format: "+f)
			t, err = time.Parse(f, ts)
			if err == nil {
				ifPrintln(6, "Match found! ^^^")
				return &t
			}
		}
	}
	return nil
}

func initialize() {
	ifPrintln(2, "Initializing subsystems and caches...")
	defer ifPrintln(2, "Subsystems and caches initialized.")

	// Acquire the Consensus download time. If importing from a file, it is
	// taken from the command line or the filename itself. If downloaded it's now()
	g_consensusDLTS = getConsensusDLTimestamp()
	ifPrintln(2, "Consensus DownloadTimestamp (DLTS) check: "+g_consensusDLTS)

	if g_config.DBServer.Enabled { // Check id DB backend is enabled
		ifPrintln(2, "Initializing all caches.")
		defer ifPrintln(2, "All caches initialized.")

		// Open DB connection
		g_db = NewDBFromConfig(g_config)

		// Initialize DB caches
		g_db.initCaches()

		// Initialize CC cache
		g_db.initCountryNameCache()

		// Initialize the Latest Relay cache - stores the latest relay before certain timestamp
		g_db.initializeLatestRelayDataCache(&g_db.lrd, g_consensusDLTS)
	}
}

func cleanup() {
	ifPrintln(5, "Starting cleanup()")
	if g_db != nil {
		g_db.Close()
	}
	ifPrintln(5, "Completed cleanup()")
}

func init() {
	// Parse command line arguments first, to find the config file path and if we are using database backend
	parseCmdlnArguments(&g_config)
}

func logDataImport(tor_response *TorResponse) {
	ifPrintln(-3, fmt.Sprintf("TOR Version, build revision: %s, %s (Aquisition time: %s)",
		tor_response.Version, tor_response.Build_revision, g_consensusDLTS))
	if g_db != nil && g_db.initialized {
		g_db.addToTorQueries(tor_response.Version, tor_response.Relays_published, tor_response.Bridges_published, g_consensusDLTS)
	}
}

func main() {
	initialize()
	defer cleanup()

	// Extract the TOR node filters from the arguments
	matchFlags := parseNodeFilters(f_nodeFilter)

	ifPrintln(1, fmt.Sprintf("Filters requested: %v", matchFlags))

	tor_response := getConsensus()
	logDataImport(&tor_response)

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

	for _, relay := range tor_response.Relays {
		ifPrintln(4, "\n== Processing node with fingerprint/nickname: "+relay.Fingerprint+"/"+relay.Nickname+" ===============================")
		// Check if this is a newer record

		if g_db != nil && g_db.initialized { // Database backend logic
			// Clean up excess space left/right
			relay.Contact = strings.TrimSpace(relay.Contact)

			// The check below needs to be segmented so subtables can be updated independently of TorRelays
			fp := relay.Fingerprint
			ifPrintln(6, "Comparing records for fingerprint: "+fp)
			if recordsMatch(relay, g_db.lrd[fp]) { // MATCH - deal with node updates in DB
				ifPrintln(4, "DEBUG: g_consensusDLTS: "+g_consensusDLTS+"; lrd[fp]['RecordLastSeen']: "+g_db.lrd[fp]["RecordLastSeen"])

				// Record Last Seen timestamps match?
				if g_consensusDLTS == g_db.lrd[fp]["RecordLastSeen"] { // Last seen matches - no updates
					ifPrintln(4, fmt.Sprintf("DEBUG: TorRelay %s records RLS TIMESTAMPS MATCH!!! No DB update need at all", fp))
				} else { // Update RecordLastSeen of TorRelay and dependent records
					ifPrintln(4, fmt.Sprintf("DEBUG: TorRelay %s records RLS TIMESTAMPS do not match. Need to check relay addresses", fp))

					// if Or, Exit and Dir have changed, however we are going to update their RLS to
					// speed up queries against those index tables.
					updateRelayAddressesIfNeeded(&relay, &g_db.lrd)

					// Update the RecordLastSeen (RLS) timestamp
					g_db.updateTorRelayRLS(g_db.lrd[fp]["id"], g_consensusDLTS)
				}
				continue
			} else { // No match/New Record/Add to DB
				addNewTorRelayToDB(relay)
			}
		}
	}
	ifPrintln(5, "DONE: parsing Consensus file")
}

func addNewTorRelayToDB(relay TorDetails) {
	ifPrintln(4, fmt.Sprintf("func addNewTorRelayToDB(%q): ", relay))
	defer ifPrintln(4, "func addNewTorRelayToDB: RETURN")

	fpid := g_db.value2id("fingerprint", relay.Fingerprint)
	countryid := g_db.normalizeCountryID(relay.Country, relay.Country_name)
	regionid := g_db.value2id("region", relay.Region_name)
	cityid := g_db.value2id("city", relay.City_name)
	platformid := g_db.value2id("platform", relay.Platform)
	versionid := g_db.value2id("version", relay.Version)
	contactid := g_db.value2id("contact", relay.Contact)

	js_exitp, _ := json.Marshal(relay.Exit_policy)
	exitp := g_db.value2id("exitp", string(js_exitp))

	js_exitps, _ := json.Marshal(relay.Exit_policy_summary)
	exitps := g_db.value2id("exitps", string(js_exitps))

	js_exitps6, _ := json.Marshal(relay.Exit_policy_v6_summary)
	exitps6 := g_db.value2id("exitps6", string(js_exitps6))

	// Store in intermediate variables before compacting the JSON object (before it's stored)
	nick := relay.Nickname
	lastChanged := relay.Last_changed_address_or_port
	firstSeen := relay.First_seen

	// Cleanup/compact the JSON object before marshaling
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

	ifPrintln(5, fmt.Sprintf("=============== INSERTING RECORD in TorRelays =================\n"+
		"fpid: %s\ncountryid: %s\nregionid: %s\ncityid: %s\nrelay.Nickname: %s\n"+
		"relay.Last_changed_address_or_port: %s\nrelay.First_seen: %s\nRecordTimeInserted: %s\nRecordLastSeen: %s\njsFlags: %s\njsRelay: %s\n",
		fpid, countryid, regionid, cityid, nick, lastChanged, firstSeen, g_consensusDLTS, g_consensusDLTS, jsFlags, jsRelay))

	res, err := g_db.stmtAddTorRelays.Exec(fpid, countryid, regionid, cityid, platformid, versionid, contactid,
		exitp, exitps, exitps6, nick, lastChanged, firstSeen, g_consensusDLTS, g_consensusDLTS, jsFlags, jsRelay)
	if err != nil {
		panic("func main: g_db.stmtAddTorRelays.Exec: " + err.Error())
	}

	lastID_int64, err := res.LastInsertId()
	lastID := fmt.Sprintf("%d", lastID_int64)
	ifPrintln(4, "TorRelay LastInsertID: "+lastID)

	// Add Or, Ex, Di addresses to the corresponding databases
	addNewRelayAddresses(lastID, fpid, relay.Or_addresses, relay.Exit_addresses, relay.Dir_address)
}

func addNewRelayAddresses(lastID string, fpid string, Or_addresses []string, Exit_addresses []string, Dir_address string) {
	ifPrintln(4, fmt.Sprintf("func addNewRelayAddresses(%s,%s,%q,%q,%s): ", lastID, fpid, Or_addresses, Exit_addresses, Dir_address))
	defer ifPrintln(4, "func addNewRelayAddresses: RETURN")

	ifPrintln(4, fmt.Sprintf("TorRelay: Loop Or_addresses: %v\n", Or_addresses))
	if len(Or_addresses) > 0 {
		for _, or := range Or_addresses {
			ifPrintln(5, "TorRelay: Or_addresses: "+or)
			g_db.addToIP("Or", fpid, g_consensusDLTS, g_consensusDLTS, or)
		}
	}

	ifPrintln(4, fmt.Sprintf("TorRelay: Loop Exit_addresses: %v\n", Exit_addresses))
	if len(Exit_addresses) > 0 {
		for _, ex := range Exit_addresses {
			ifPrintln(5, "TorRelay: Exit_addresses: "+ex)
			g_db.addToIP("Ex", fpid, g_consensusDLTS, g_consensusDLTS, ex)
		}
	}

	// relay.Dir_address is a string not an array
	ifPrintln(4, "TorRelay: Dir_addresses: "+Dir_address)
	if len(Dir_address) > 0 {
		g_db.addToIP("Di", fpid, g_consensusDLTS, g_consensusDLTS, Dir_address)
	}
}

func updateRelayAddressesIfNeeded(relay *TorDetails, lrd *map[string](map[string]string)) {
	ifPrintln(4, "func updateRelayAddressesIfNeeded(BEGIN): ")
	defer ifPrintln(4, "func updateRelayAddressesIfNeeded: RETURN")

	ifPrintln(6, "Checking OR...")
	fp := (*relay).Fingerprint
	if len(relay.Or_addresses) > 0 {
		for _, or := range relay.Or_addresses {
			//fmt.Println("TorRelay: Or_addresses: " + or)
			g_db.updateIfNeededRelayAddressRLS("Or", (*lrd)[fp]["ID_NodeFingerprints"], g_consensusDLTS, or)
		}
	}

	ifPrintln(6, "Checking Exit...")
	if len(relay.Exit_addresses) > 0 {
		for _, ex := range relay.Exit_addresses {
			//fmt.Println("TorRelay: Exit_addresses" + ex)
			g_db.updateIfNeededRelayAddressRLS("Ex", (*lrd)[fp]["ID_NodeFingerprints"], g_consensusDLTS, ex)
		}
	}

	ifPrintln(6, "Checking Directory...")
	if len(relay.Dir_address) > 0 {
		//fmt.Println("TorRelay: Dir_addresses" + relay.Dir_address)
		g_db.updateIfNeededRelayAddressRLS("Di", (*lrd)[fp]["ID_NodeFingerprints"], g_consensusDLTS, relay.Dir_address)
	}
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
		relay.First_seen == lrdfp["First_seen"] &&
		string(js_exitp) == lrdfp["ExitPolicy"] &&
		string(js_exitps) == lrdfp["ExitPolicySummary"] &&
		string(js_exitps6) == lrdfp["ExitPolicyV6Summary"] {

		ifPrintln(3, "MATCHED: "+lrdfp["Fingerprint"])
		return true
	} else {
		// #### Just for debugging - delete later
		ifPrintln(0, "NO MATCH: "+lrdfp["Fingerprint"])
		if g_config.Verbosity >= 6 {
			fmt.Println("(Current Relay data => LRD Cache data)")
			fmt.Printf("Fingerprint: %s => %s\n", relay.Fingerprint, lrdfp["Fingerprint"])
			fmt.Printf("Nickname: %s => %s\n", relay.Nickname, lrdfp["Nickname"])

			if relay.Country != lrdfp["Country"] {
				fmt.Printf("FAIL Country: %s => %s\n", relay.Country, lrdfp["Country"])
			}
			if relay.City_name != lrdfp["CityName"] {
				fmt.Printf("FAIL City Name: %s => %s\n", relay.City_name, lrdfp["CityName"])
			}
			if relay.Platform != lrdfp["PlatformName"] {
				fmt.Printf("FAIL Platform: %s => %s\n", relay.Platform, lrdfp["PlatformName"])
			}
			if relay.Version != lrdfp["VersionName"] {
				fmt.Printf("FAIL Version: %s => %s (%s)\n", relay.Version, lrdfp["VersionName"], lrdfp["ID_Versions"])
			}
			if strings.ToLower(relay.Contact) != strings.ToLower(lrdfp["ContactName"]) {
				fmt.Printf("FAIL Contact: %s => %s (%s)\n", relay.Contact, lrdfp["ContactName"], lrdfp["ID_Contacts"])
			}
			if relay.Last_changed_address_or_port != lrdfp["Last_changed_address_or_port"] {
				fmt.Printf("FAIL LastCHAP: %s => %s\n", relay.Last_changed_address_or_port, lrdfp["Last_changed_address_or_port"])
			}
			if relay.First_seen != lrdfp["First_seen"] {
				fmt.Printf("FAIL FirstSeen: %s => %s\n", relay.First_seen, lrdfp["First_seen"])
			}
			/*
				string(js_exitp) == lrdfp["ExitPolicy"] &&
				string(js_exitps) == lrdfp["ExitPolicySummary"] &&
				string(js_exitps6) == lrdfp["ExitPolicyV6Summary"]
			*/
		}
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

func parseConfigFile(cfgFilename string, cfg *TorHistoryConfig) {
	ifPrintln(1, "Reading configuration file: "+cfgFilename)
	if cfgFilename == "" {
		return
	}
	f, err := os.Open(cfgFilename)
	if err != nil {
		log.Fatalf("Unable to open configuration file: %s\n", cfgFilename)
	}
	defer f.Close()

	cmdLineVerbosity := cfg.Verbosity // Preserve verbosity from the command line (if 0 - not set)
	// after the config file is read, it will overwrite the global verbosity variable which may have been set by a command line argument
	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(cfg)
	if err != nil {
		log.Fatalf("YAML Decoder error: %s\n", err)
	}
	if cmdLineVerbosity != 0 { // Restore verbosity level set by command line (if it was set)
		cfg.Verbosity = cmdLineVerbosity
	}
	ifPrintln(-8, fmtDBCfg(*cfg, true))
}

func fmtDBCfg(cfg TorHistoryConfig, hidePassword bool) string {
	var pwd string
	if hidePassword {
		pwd = "<redacted>"
	} else {
		pwd = cfg.DBServer.Password
	}
	return fmt.Sprintf("Database configutation:  Host: %s\n  Port: %s\n  DB Name: %s\n  Username: %s\n  Password: %s",
		cfg.DBServer.Host, cfg.DBServer.Port, cfg.DBServer.DBName, cfg.DBServer.Username, pwd)
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
	if len(*needles) == 0 { // Optimization - if no needles - always true
		return true
	}
NeedleLoop:
	for _, curNeedle := range *needles {
		for _, curStr := range *set {
			if curStr == curNeedle {
				continue NeedleLoop
			}
		}
		return false
	}
	return true
}

func parseCmdlnArguments(cfg *TorHistoryConfig) {
	// Read verbosity from command line
	verbosity := flag.Uint("verbosity", 0, "Verbosity level. If negative print to Stderr")
	quiet := flag.Bool("quiet", false, "Suppreses all verbocity")

	// Read config filename if one provided
	cfgFilename := flag.String("config-filename", "", "Full path of YAML config file")

	input := flag.String("input-data-file", "", "Use input file instead of downloading from the consensus")
	backup := flag.String("consensus-backup-file", "", "Make a backup of the consensus as downloaded at the supplied destination path/prefix. Timestamp is automatically appended.")
	backupGzip := flag.Bool("consensus-backup-gzip", false, "GZip the backup file")

	f_nodeFilter = flag.String("node-flag", "", "Node flag filter: BadExit, Exit, Fast, Guard, HSDir, Running, Stable, StaleDesc, V2Dir and Valid")
	f_nodeInfo = flag.Bool("node-info", true, "Generic node information (on by default)")
	f_expandIPs = flag.Bool("expand-ips", false, "Forces one per line expansion of the IPs in the answer section")
	f_expandIPsAndFlags = flag.Bool("expand-ips-flags", false, "Forces one per line expansion of the IPs in the answer section")

	consensusDownloadTime := flag.String("consensus-download-time", "", "The time the consensus was downloaded. Useful when importing data downloaded in the past")
	consensusDownloadTime_fmt := flag.String("consensus-download-time-format", "", "The time the consensus was downloaded. Useful when importing data downloaded in the past")
	extractCDLTfromFilename := flag.Bool("extract-consensus-download-time-from-filename", false, "When importing from a file, it attempts to read the consensus download date from the filename")
	extractCDLTfromFilenameRegEx := flag.String("filename-regex", "", "When importing from a file and attempting to extract the timestamp from its name, this regex will be used")

	flag.Parse()
	cfg.Verbosity = *verbosity
	cfg.Quiet = *quiet
	// figure variable overriding from cmd line
	if *cfgFilename != "" { // Read config file if one supplied
		parseConfigFile(*cfgFilename, cfg)
	}

	if *backup != "" { // If backup file ame and compression supplied on command line
		g_config.Backup.Filename = *backup
		g_config.Backup.Gzip = *backupGzip
	}

	if *input != "" { // This overrides download
		g_config.Tor.Filename = *input
	}

	if cfg.Tor.ConsensusURL == "" {
		ifPrintln(-1, "Adding default consensus URL")
		cfg.Tor.ConsensusURL = g_consensus_details_URL
	}

	cfg.Tor.ConsensusDLT = *consensusDownloadTime
	cfg.Tor.ConsensusDLT_fmt = *consensusDownloadTime_fmt
	cfg.Tor.ExtractDLTfromFilename = *extractCDLTfromFilename
	cfg.Tor.ExtractDLTfromFilename_regex = *extractCDLTfromFilenameRegEx

	if len(cfg.Tor.ExtractDLTfromFilename_regex) > 0 { // If regex for file extraction is specified then force file extraction bit
		cfg.Tor.ExtractDLTfromFilename = true
	}

	// Validate DB arguments
	if cfg.DBServer.Host != "" && cfg.DBServer.Port != "" && cfg.DBServer.DBName != "" && cfg.DBServer.Username != "" {
		cfg.DBServer.Enabled = true
	} else if cfg.DBServer.Host != "" || cfg.DBServer.Port != "" || cfg.DBServer.DBName != "" || cfg.DBServer.Username != "" || cfg.DBServer.Password != "" {
		log.Fatal("Incomplete database configuation.\n" + fmtDBCfg(*cfg, true) + "\n")
	}

	////****************************************************
	// Disable f_expandIPs if f_expandIPsAndFlags is on
	if *f_expandIPsAndFlags {
		*f_expandIPs = false
	}

	if cfg.Verbosity > 4 && len(flag.Args()) > 0 {
		fmt.Println(os.Stderr, "DEBUG: Unprocessed args:", flag.Args())
	}
	//	ifPrintln(2, fmt.Sprintf("%v\n", *cfg))
}

func parseNodeFilters(f_nodeFilter *string) []string {
	var matchFlags []string
	if *f_nodeFilter == "" {
		if g_config.Verbosity > 4 {
			fmt.Fprintln(os.Stderr, "No filters were applied")
		}
	} else {
		matchFlags = strings.Split(*f_nodeFilter, ",")
		if g_config.Verbosity > 4 {
			fmt.Fprintf(os.Stderr, "DEBUG: nodeFlag(s) in filter: ")
			for _, i := range matchFlags {
				fmt.Fprintf(os.Stderr, " %s", i)
			}
			fmt.Fprintf(os.Stderr, "\n")
		}
	}
	return matchFlags
}

func backupIfRequested(data []byte) {
	// Check if backup is requested
	if g_config.Backup.Filename == "" {
		ifPrintln(-5, "No backup requested.")
	} else {
		ifPrintln(-5, "Backup requested.")
		backupConsensus(data)
	}
}

func getConsensus() TorResponse {
	var data []byte
	if g_config.Tor.Filename != "" {
		data = readConsensusDataFromFile(g_config.Tor.Filename)
	} else {
		data = downloadConsensus(g_config.Tor.ConsensusURL)
	}

	backupIfRequested(data)

	// Parse json
	consensusData := json.NewDecoder(bytes.NewReader(data))

	var tor_response TorResponse
	err := consensusData.Decode(&tor_response)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parsing Consensus file: %s", err.Error())
		log.Fatal(err)
	}

	return tor_response
}

func readConsensusDataFromFile(fn string) []byte {
	ifPrintln(1, "readConsensusDataFromFile(\""+fn+"\"): ")
	defer ifPrintln(1, "readConsensusDataFromFile complete.")

	dataFile, err := ioutil.ReadFile(fn)
	if err != nil {
		log.Fatal("ERROR: opening Consensus data file (%s). ", err.Error())
	}
	return dataFile
}

func downloadConsensus(url string) []byte {
	ifPrintln(2, "Downloading Consensus details from: "+url)
	defer ifPrintln(2, "Consensus download complete.")

	http_session, err := http.Get(url)
	if err != nil {
		log.Fatal(err)
	}
	defer http_session.Body.Close()

	data, err := ioutil.ReadAll(http_session.Body)
	if err != nil {
		log.Fatal(err)
	}
	return data
}

func backupConsensus(data []byte) {
	ifPrintln(2, "backupConsensus: ")
	defer ifPrintln(2, "backupConsensus complete.")

	t := time.Now().UTC()
	fn := g_config.Backup.Filename + "-" + t.Format("20060102150405")
	if g_config.Backup.Gzip {
		fn += ".gz"
	}

	ifPrintln(3, "Creating backup file: "+fn)
	backup_file, _ := os.Create(fn)
	defer backup_file.Close()

	if g_config.Backup.Gzip {
		zw := gzip.NewWriter(backup_file)
		zw.Name = fn
		zw.ModTime = time.Now()
		zw.Comment = "tor-nodes"

		_, err := zw.Write(data)
		if err != nil {
			log.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			log.Fatal(err)
		}
	} else {
		backup_file.Write(data)
	}
}
