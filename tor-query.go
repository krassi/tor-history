package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"

	"github.com/sensepost/maltegolocal/maltegolocal"
)

var g_db *DB
var g_consensusDLTS string = ""

type TorRelayDetails struct {
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

	DBServer struct {
		Enabled      bool   //`yaml:"enabled"`
		Port         string `yaml:"port"`
		Host         string `yaml:"host"`
		DBName       string `yaml:"database"`
		Username     string `yaml:"username"`
		Password     string `yaml:"password"`
		ReInitCaches int    `yaml:"reinit-caches"`
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
	Print struct {
		Separator      string
		Nickname       bool
		Fingerprint    bool
		Or_addresses   bool
		Exit_addresses bool
		Dir_address    bool
		Country        bool
		AS             bool
		Hostname       bool
		Flags          bool
		IPperLine      bool
	} `yaml:"Print"`
	Filter struct {
		Running     bool
		Hibernating bool
		matchFlags  []string
	}
}

var g_config TorHistoryConfig

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

func main() {
	defer cleanup()

	lt := maltegolocal.ParseLocalArguments(os.Args)
	EntityValue := lt.Value
	TRX := maltegolocal.MaltegoTransform{}

	args := ""
	for k, v := range lt.Values {
		args += k + "=>" + v + "; "
	}

	//	g_db = NewDB(cfg.DBServer.Username, cfg.DBServer.Password, cfg.DBServer.Host, cfg.DBServer.Port, cfg.DBServer.DBName)
	g_db = NewDB("tor-rw", "", "localhost", "3306", "tor_history")

	for k, v := range lt.Values {
		switch k {
		case "countrysc": // Maltego typ country field
			EntityValue = strings.ToLower(v)
			lookupByCC(&TRX, EntityValue)
			break
		case "properties.shodan.country": // Shodan type country field
			EntityValue = strings.ToLower(EntityValue)
			lookupByCC(&TRX, EntityValue)
			break
		case "ipv4-address":
			lookupByIP(&TRX, EntityValue)
			break
		case "email":
			lookupByEmail(&TRX, EntityValue)
			break
		}
	}
	TRX.AddUIMessage("completed!", "Inform")
	fmt.Println(TRX.ReturnOutput())
}

func concatIDs(ids map[string]string) string {
	idList := ""
	for id, _ := range ids {
		idList += id + ", "
	}
	if len(idList) > 1 {
		idList = idList[0 : len(idList)-2]
	}
	return idList
}

func lookupByEmail(TRX *maltegolocal.MaltegoTransform, EntityValue string) {
	ids := g_db.getLatestTRsIDsByEmail(EntityValue)

	TRX.AddUIMessage(fmt.Sprintf("Records matching: %d\n", len(ids)), "Inform")
	idList := concatIDs(ids)

	relays := g_db.getTorRelaysByIDStringList(idList)
	for _, relay := range relays {
		createMaltegoNode(TRX, relay)
	}
}

func lookupByIP(TRX *maltegolocal.MaltegoTransform, EntityValue string) {
	ids := g_db.getLatestTRsIDsByIP(EntityValue)

	TRX.AddUIMessage(fmt.Sprintf("Records matching: %d\n", len(ids)), "Inform")
	idList := concatIDs(ids)

	relays := g_db.getTorRelaysByIDStringList(idList)
	for _, relay := range relays {
		createMaltegoNode(TRX, relay)
	}
}

func lookupByCC(TRX *maltegolocal.MaltegoTransform, EntityValue string) {
	ids := g_db.getLatestTRsIDsByCountryCode(EntityValue)
	TRX.AddUIMessage(fmt.Sprintf("Records matching: %d\n", len(ids)), "Inform")
	idList := concatIDs(ids)

	TRX.AddUIMessage("DEBUG: IDs list: "+idList, "Inform")
	relays := g_db.getTorRelaysByIDStringList(idList)
	for _, relay := range relays {
		createMaltegoNode(TRX, relay)
	}
}

func createMaltegoNode(TRX *maltegolocal.MaltegoTransform, relay map[string]string) {
	BaseEnt := TRX.AddEntity("ktt.TORNode", relay["Nickname"]+"\n"+relay["Fingerprint"])
	for k, v := range relay {
		if k == "ID" {
			continue
		}
		BaseEnt.AddProperty(k, "", "nostrict", v)
	}
	// Dynamic properties
	var details TorRelayDetails
	err := json.Unmarshal([]byte(relay["jsd"]), &details)
	if err != nil {
		TRX.AddUIMessage(fmt.Sprintf("Problem unmarshalling: %s\n %s", relay["jsd"], err), "Inform")
	}
	for _, ip := range details.Exit_addresses {
		BaseEnt.AddProperty("Exit_addresses", "Exit Address", "nostrict", ip)
	}
	for _, ip := range details.Or_addresses {
		BaseEnt.AddProperty("Or_addresses", "Router Address", "nostrict", ip)
	}
	if len(details.Dir_address) > 0 {
		BaseEnt.AddProperty("Dir_addresses", "Directory Address", "nostrict", details.Dir_address)
	}
}

func cleanup() {
	ifPrintln(5, "Starting cleanup()")
	if g_db != nil {
		g_db.Close()
	}
	ifPrintln(5, "Completed cleanup()")
}
