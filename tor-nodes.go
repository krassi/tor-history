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
	"net/http"
	"os"
	"strings"
	"time"
)

type TorResponse struct {
	Version                      string // required; Onionoo protocol version string.
	Next_major_version_scheduled string // optional; UTC date (YYYY-MM-DD) when the next major protocol version is scheduled to be deployed. Omitted if no major protocol changes are planned.
	Build_revision               string // optional # Git revision of the Onionoo instance's software used to write this response, which will be omitted if unknown.
	Relays_published             string // required # UTC timestamp (YYYY-MM-DD hh:mm:ss) when the last known relay network status consensus started being valid. Indicates how recent the relay objects in this document are.
	Relays_skipped               uint64 // optional # Number of skipped relays as requested by a positive "offset" parameter value. Omitted if zero.
	Relays                       []TorDetails
	//Relays []interface{}
	//	Relays array of objects // required # Array of relay objects as specified below.

	Relays_truncated  uint64 // optional # Number of truncated relays as requested by a positive "limit" parameter value. Omitted if zero.
	Bridges_published string // required # UTC timestamp (YYYY-MM-DD hh:mm:ss) when the last known bridge network status was published. Indicates how recent the bridge objects in this document are.
	Bridges_skipped   uint64 // optional # Number of skipped bridges as requested by a positive "offset" parameter value. Omitted if zero.
	Bridges           []interface{}
	//	Bridges array of objects // required # Array of bridge objects as specified below.
	Bridges_truncated uint64 // optional # Number of truncated bridges as requested by a positive "limit" parameter value. Omitted if zero.
}

type TorDetails struct {
	Nickname                     string      // required # Relay nickname consisting of 1–19 alphanumerical characters. Turned into required field on March 14, 2018.
	Fingerprint                  string      // required # Relay fingerprint consisting of 40 upper-case hexadecimal characters.
	Or_addresses                 []string    // required # Array of IPv4 or IPv6 addresses and TCP ports or port lists where the relay accepts onion-routing connections. The first address is the primary onion-routing address that the relay used to register in the network, subsequent addresses are in arbitrary order. IPv6 hex characters are all lower-case.
	Exit_addresses               []string    // optional # Array of IPv4 addresses that the relay used to exit to the Internet in the past 24 hours. Omitted if array is empty. Changed on April 17, 2018 to include all exit addresses, regardless of whether they are used as onion-routing addresses or not.
	Dir_address                  string      // optional # IPv4 address and TCP port where the relay accepts directory connections. Omitted if the relay does not accept directory connections.
	Last_seen                    string      // required # UTC timestamp (YYYY-MM-DD hh:mm:ss) when this relay was last seen in a network status consensus.
	Last_changed_address_or_port string      // required # UTC timestamp (YYYY-MM-DD hh:mm:ss) when this relay last stopped announcing an IPv4 or IPv6 address or TCP port where it previously accepted onion-routing or directory connections. This timestamp can serve as indicator whether this relay would be a suitable fallback directory.
	First_seen                   string      // required # UTC timestamp (YYYY-MM-DD hh:mm:ss) when this relay was first seen in a network status consensus.
	Running                      bool        // required # Boolean field saying whether this relay was listed as running in the last relay network status consensus.
	Hibernating                  bool        // optional # Boolean field saying whether this relay indicated that it is hibernating in its last known server descriptor. This information may be helpful to decide whether a relay that is not running anymore has reached its accounting limit and has not dropped out of the network for another, unknown reason. Omitted if either the relay is not hibernating, or if no information is available about the hibernation status of the relay.
	Flags                        []string    // optional # Array of relay flags that the directory authorities assigned to this relay. May be omitted if empty.
	Country                      string      // optional # Two-letter lower-case country code as found in a flagsIP database by resolving the relay's first onion-routing IP address. Omitted if the relay IP address could not be found in the GeoIP database.
	Country_name                 string      // optional # Country name as found in a GeoIP database by resolving the relay's first onion-routing IP address. Omitted if the relay IP address could not be found in the GeoIP database, or if the GeoIP database did not contain a country name.
	Region_name                  string      // optional # Region name as found in a GeoIP database by resolving the relay's first onion-routing IP address. Omitted if the relay IP address could not be found in the GeoIP database, or if the GeoIP database did not contain a region name.
	City_name                    string      // optional # City name as found in a GeoIP database by resolving the relay's first onion-routing IP address. Omitted if the relay IP address could not be found in the GeoIP database, or if the GeoIP database did not contain a city name.
	Latitude                     float64     // optional # Latitude as found in a GeoIP database by resolving the relay's first onion-routing IP address. Omitted if the relay IP address could not be found in the GeoIP database.
	Longitude                    float64     // optional # Longitude as found in a GeoIP database by resolving the relay's first onion-routing IP address. Omitted if the relay IP address could not be found in the GeoIP database.
	As                           string      // optional # AS number as found in an AS database by resolving the relay's first onion-routing IP address. AS number strings start with "AS", followed directly by the AS number. Omitted if the relay IP address could not be found in the AS database. Added on August 3, 2018.
	As_number                    string      // optional # AS number as found in an AS database by resolving the relay's first onion-routing IP address. AS number strings start with "AS", followed directly by the AS number. Omitted if the relay IP address could not be found in the AS database. Removed on September 10, 2018.
	As_name                      string      // optional # AS name as found in an AS database by resolving the relay's first onion-routing IP address. Omitted if the relay IP address could not be found in the AS database.
	Consensus_weight             uint64      // required # Weight assigned to this relay by the directory authorities that clients use in their path selection algorithm. The unit is arbitrary; currently it's kilobytes per second, but that might change in the future.
	Host_name                    string      // optional # Host name as found in a reverse DNS lookup of the relay's primary IP address. This field is updated at most once in 12 hours, unless the relay IP address changes. Omitted if the relay IP address was not looked up, if no lookup request was successful yet, or if no A record was found matching the PTR record. Deprecated on July 16, 2018.
	Verified_host_names          []string    // optional # Host names as found in a reverse DNS lookup of the relay's primary IP address for which a matching A record was also found. This field is updated at most once in 12 hours, unless the relay IP address changes. Omitted if the relay IP address was not looked up, if no lookup request was successful yet, or if no A records were found matching the PTR records (i.e. it was not possible to verify the value of any of the PTR records). A DNSSEC validating resolver is used for these lookups. Failure to validate DNSSEC signatures will prevent those names from appearing in this field. Added on July 16, 2018. Updated to clarify that a DNSSEC validating resolver is used on August 17, 2018.
	Unverified_host_names        []string    // optional # Host names as found in a reverse DNS lookup of the relay's primary IP address that for which a matching A record was not found. This field is updated at most once in 12 hours, unless the relay IP address changes. Omitted if the relay IP address was not looked up, if no lookup request was successful yet, or if A records were found matching all PTR records (i.e. it was possible to verify the value of each of the PTR records). A DNSSEC validating resolver is used for these lookups. Failure to validate DNSSEC signatures will prevent those names from appearing in this field. Added on July 16, 2018. Updated to clarify that a DNSSEC validating resolver is used on August 17, 2018.
	Last_restarted               string      // optional # UTC timestamp (YYYY-MM-DD hh:mm:ss) when the relay was last (re-)started. Missing if router descriptor containing this information cannot be found.
	Bandwidth_rate               uint64      // optional # Average bandwidth in bytes per second that this relay is willing to sustain over long periods. Missing if router descriptor containing this information cannot be found.
	Bandwidth_burst              uint64      // optional # Bandwidth in bytes per second that this relay is willing to sustain in very short intervals. Missing if router descriptor containing this information cannot be found.
	Observed_bandwidth           uint64      // optional # Bandwidth estimate in bytes per second of the capacity this relay can handle. The relay remembers the maximum bandwidth sustained output over any ten second period in the past day, and another sustained input. The "observed_bandwidth" value is the lesser of these two numbers. Missing if router descriptor containing this information cannot be found.
	Advertised_bandwidth         uint64      // optional # Bandwidth in bytes per second that this relay is willing and capable to provide. This bandwidth value is the minimum of bandwidth_rate, bandwidth_burst, and observed_bandwidth. Missing if router descriptor containing this information cannot be found.
	Exit_policy                  []string    // optional # Array of exit-policy lines. Missing if router descriptor containing this information cannot be found. May contradict the "exit_policy_summary" field in a rare edge case: this happens when the relay changes its exit policy after the directory authorities summarized the previous exit policy.
	Exit_policy_summary          interface{} // optional # Summary version of the relay's exit policy containing a dictionary with either an "accept" or a "reject" element. If there is an "accept" ("reject") element, the relay accepts (rejects) all TCP ports or port ranges in the given list for most IP addresses and rejects (accepts) all other ports. May contradict the "exit_policy" field in a rare edge case: this happens when the relay changes its exit policy after the directory authorities summarized the previous exit policy.
	Exit_policy_v6_summary       interface{} // optional # Summary version of the relay's IPv6 exit policy containing a dictionary with either an "accept" or a "reject" element. If there is an "accept" ("reject") element, the relay accepts (rejects) all TCP ports or port ranges in the given list for most IP addresses and rejects (accepts) all other ports. Missing if the relay rejects all connections to IPv6 addresses. May contradict the "exit_policy_summary" field in a rare edge case: this happens when the relay changes its exit policy after the directory authorities summarized the previous exit policy.
	Contact                      string      // optional # Contact address of the relay operator. Omitted if empty or if descriptor containing this information cannot be found.
	Platform                     string      // optional # Platform string containing operating system and Tor version details. Omitted if empty or if descriptor containing this information cannot be found.
	Version                      string      // optional # Tor software version without leading "Tor" as reported by the directory authorities in the "v" line of the consensus. Omitted if either the directory authorities or the relay did not report which version the relay runs or if the relay runs an alternative Tor implementation.
	Recommended_version          bool        // optional # Boolean field saying whether the Tor software version of this relay is recommended by the directory authorities or not. Uses the relay version in the consensus. Omitted if either the directory authorities did not recommend versions, or the relay did not report which version it runs.
	Version_status               string      // optional # Status of the Tor software version of this relay based on the versions recommended by the directory authorities. Possible version statuses are: "recommended" if a version is listed as recommended; "experimental" if a version is newer than every recommended version; "obsolete" if a version is older than every recommended version; "new in series" if a version has other recommended versions with the same first three components, and the version is newer than all such recommended versions, but it is not newer than every recommended version; "unrecommended" if none of the above conditions hold. Omitted if either the directory authorities did not recommend versions, or the relay did not report which version it runs. Added on April 6, 2018.
	Effective_family             []string    // optional # Array of fingerprints of relays that are in an effective, mutual family relationship with this relay. These relays are part of this relay's family and they consider this relay to be part of their family. Always contains the relay's own fingerprint. Omitted if the descriptor containing this information cannot be found. Updated to always include the relay's own fingerprint on March 14, 2018.
	Alleged_family               []string    // optional # Array of fingerprints of relays that are not in an effective, mutual family relationship with this relay. These relays are part of this relay's family but they don't consider this relay to be part of their family. Omitted if empty or if descriptor containing this information cannot be found.
	Indirect_family              []string    // optional # Array of fingerprints of relays that are not in an effective, mutual family relationship with this relay but that can be reached by following effective, mutual family relationships starting at this relay. Omitted if empty or if descriptor containing this information cannot be found.
	Consensus_weight_fraction    float64     // optional # Fraction of this relay's consensus weight compared to the sum of all consensus weights in the network. This fraction is a very rough approximation of the probability of this relay to be selected by clients. Omitted if the relay is not running.
	Guard_probability            float64     // optional # Probability of this relay to be selected for the guard position. This probability is calculated based on consensus weights, relay flags, and bandwidth weights in the consensus. Path selection depends on more factors, so that this probability can only be an approximation. Omitted if the relay is not running, or the consensus does not contain bandwidth weights.
	Middle_probability           float64     // optional # Probability of this relay to be selected for the middle position. This probability is calculated based on consensus weights, relay flags, and bandwidth weights in the consensus. Path selection depends on more factors, so that this probability can only be an approximation. Omitted if the relay is not running, or the consensus does not contain bandwidth weights.
	Exit_probability             float64     // optional # Probability of this relay to be selected for the exit position. This probability is calculated based on consensus weights, relay flags, and bandwidth weights in the consensus. Path selection depends on more factors, so that this probability can only be an approximation. Omitted if the relay is not running, or the consensus does not contain bandwidth weights.
	Measured                     bool        // optional # Boolean field saying whether the consensus weight of this relay is based on a threshold of 3 or more measurements by Tor bandwidth authorities. Omitted if the network status consensus containing this relay does not contain measurement information.
	Unreachable_or_addresses     []string    // optional # Array of IPv4 or IPv6 addresses and TCP ports or port lists where the relay claims in its descriptor to accept onion-routing connections but that the directory authorities failed to confirm as reachable. Contains only additional addresses of a relay that are found unreachable and only as long as a minority of directory authorities performs reachability tests on these additional addresses. Relays with an unreachable primary address are not included in the network status consensus and excluded entirely. Likewise, relays with unreachable additional addresses tested by a majority of directory authorities are not included in the network status consensus and excluded here, too. If at any point network status votes will be added to the processing, relays with unreachable addresses will be included here. Addresses are in arbitrary order. IPv6 hex characters are all lower-case. Omitted if empty.
}

var g_consensus_details_URL = "https://onionoo.torproject.org/details"

var f_inputFileName, f_consensusBackupFileName *string
var f_nodeFilter *string
var f_nodeInfo *bool
var f_expandIPs, f_expandIPsAndFlags *bool
var f_debug *bool

func main() {
	// Parse command line arguments
	parseArguments()

	// Extract the TOR node filters from the arguments
	matchFlags := parseNodeFilters(f_nodeFilter)

	var tor_response TorResponse

	var jsonParser *json.Decoder

	if *f_inputFileName != "" {
		consensus_data, err := os.Open(*f_inputFileName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: opening Consensus data file (%s). ", err.Error())
			log.Fatal(err)
		}
		jsonParser = json.NewDecoder(consensus_data)
	} else {
		jsonParser = downloadConsensus(g_consensus_details_URL)
	}

	err := jsonParser.Decode(&tor_response)

	if err != nil {
		fmt.Fprintf(os.Stderr, "parsing config file: %s", err.Error())
		log.Fatal(err)
	} else {
		fmt.Fprintf(os.Stderr, "TOR Version, build revision: %s, %s\n", tor_response.Version, tor_response.Build_revision)

		for _, relay := range tor_response.Relays {
			if allStringsInSet(&matchFlags, &relay.Flags) {
				if *f_nodeInfo {
					fmt.Printf("NODE: %s (%s) => %s\n", relay.Nickname, relay.Flags, relay.Exit_addresses)
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
		}

		fmt.Printf("parsing config file")
	}

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
				//found = true
				//				fmt.Println("needle match")
				//break
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

func parseArguments() {
	f_inputFileName = flag.String("input-data-file", "", "Use input file instead of downloading from the consensus")
	f_consensusBackupFileName = flag.String("consensus-backup-file", "", "Make a backup of the consensus as downloaded at the supplied destination path/prefix.")
	f_nodeFilter = flag.String("node-flag", "", "Node flag filter: BadExit, Exit, Fast, Guard, HSDir, Running, Stable, StaleDesc, V2Dir and Valid")
	f_nodeInfo = flag.Bool("node-info", true, "Generic node information (on by default)")
	f_expandIPs = flag.Bool("expand-ips", false, "Forces one per line expansion of the IPs in the answer section")
	f_expandIPsAndFlags = flag.Bool("expand-ips-flags", false, "Forces one per line expansion of the IPs in the answer section")
	f_debug = flag.Bool("debug", true, "Debug (true/false)")

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
	if *f_debug {
		fmt.Fprintf(os.Stderr, "Downloading Consensus details from: %s", url)
	}

	http_session, err := http.Get(url)
	if err != nil {
		log.Fatal(err)
	}
	defer http_session.Body.Close()

	// Check if backup is requested
	if *f_consensusBackupFileName == "" {
		if *f_debug {
			fmt.Println(os.Stderr, "No backup requested.")
		}
		return json.NewDecoder(http_session.Body)
	} else {
		if *f_debug {
			fmt.Println(os.Stderr, "Backup requested.")
		}

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
