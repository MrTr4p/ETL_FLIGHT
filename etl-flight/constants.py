nationalities = [
    "American", "British", "Canadian", "Australian", "German", "French", 
    "Italian", "Spanish", "Portuguese", "Dutch", "Swedish", "Norwegian",
    "Danish", "Finnish", "Russian", "Chinese", "Japanese", "Korean",
    "Indian", "Brazilian", "Mexican", "Argentinian", "Chilean", "Colombian",
    "South African", "Egyptian", "Nigerian", "Kenyan", "Moroccan", "Turkish",
    "Greek", "Polish", "Ukrainian", "Irish", "Scottish", "Welsh",
    "Austrian", "Swiss", "Belgian", "Czech", "Hungarian", "Romanian",
    "Bulgarian", "Croatian", "Serbian", "Slovak", "Slovenian", "Lithuanian",
    "Latvian", "Estonian", "Palestinian", "Saudi Arabian", "Emirati", "Qatari",
    "Kuwaiti", "Omani", "Lebanese", "Jordanian", "Syrian", "Iraqi",
    "Iranian", "Pakistani", "Bangladeshi", "Sri Lankan", "Nepalese", "Bhutanese",
    "Thai", "Vietnamese", "Indonesian", "Malaysian", "Singaporean", "Filipino",
    "New Zealander", "Fijian", "Peruvian", "Venezuelan", "Ecuadorian", "Bolivian",
    "Paraguayan", "Uruguayan", "Costa Rican", "Panamanian", "Jamaican", "Bahamian",
    "Barbadian", "Trinidadian", "Algerian", "Tunisian", "Ghanaian", "Ugandan",
    "Tanzanian", "Zimbabwean", "Zambian", "Malawian", "Mozambican", "Angolan",
    "Namibian", "Botswanan", "Mauritian", "Malagasy", "Ethiopian", "Somali",
    "Sudanese", "Senegalese", "Ivorian", "Cameroonian", "Congolese", "Liberian"
]

usa_iata_codes = [
    "ABE", "ABI", "ABQ", "ABR", "ABY", "ACK", "ACT", "ACV", "ACY", "ADK",
    "ADQ", "AEX", "AGS", "AKN", "ALB", "ALO", "AMA", "ANC", "APN", "ASE",
    "ATL", "ATW", "AUS", "AVL", "AVP", "AZO", "BDL", "BET", "BFL", "BGM",
    "BGR", "BHM", "BIL", "BIS", "BJI", "BLI", "BMI", "BNA", "BOI", "BOS",
    "BPT", "BQK", "BQN", "BRD", "BRO", "BRW", "BTM", "BTR", "BTV", "BUF",
    "BUR", "BWI", "BZN", "CAE", "CAK", "CDC", "CDV", "CEC", "CHA", "CHO",
    "CHS", "CID", "CIU", "CLD", "CLE", "CLL", "CLT", "CMH", "CMI", "CMX",
    "CNY", "COD", "COS", "COU", "CPR", "CRP", "CRW", "CSG", "CVG", "CWA",
    "DAB", "DAL", "DAY", "DBQ", "DCA", "DEN", "DFW", "DHN", "DIK", "DLG",
    "DLH", "DRO", "DSM", "DTW", "DVL", "EAU", "ECP", "EGE", "EKO", "ELM",
    "ELP", "ERI", "ESC", "EUG", "EVV", "EWN", "EWR", "EYW", "FAI", "FAR",
    "FAT", "FAY", "FCA", "FLG", "FLL", "FNT", "FSD", "FSM", "FWA", "GCC",
    "GCK", "GEG", "GFK", "GGG", "GJT", "GNV", "GPT", "GRB", "GRI", "GRK",
    "GRR", "GSO", "GSP", "GST", "GTF", "GTR", "GUC", "GUM", "HDN", "HIB",
    "HLN", "HNL", "HOB", "HOU", "HPN", "HRL", "HSV", "HYA", "HYS", "IAD",
    "IAG", "IAH", "ICT", "IDA", "ILG", "ILM", "IMT", "IND", "INL", "ISN",
    "ISP", "ITH", "ITO", "JAC", "JAN", "JAX", "JFK", "JLN", "JMS", "JNU",
    "KOA", "KTN", "LAN", "LAR", "LAS", "LAW", "LAX", "LBB", "LBE", "LCH",
    "LEX", "LFT", "LGA", "LGB", "LIH", "LIT", "LNK", "LRD", "LSE", "LWS",
    "MAF", "MBS", "MCI", "MCO", "MDT", "MDW", "MEI", "MEM", "MFE", "MFR",
    "MGM", "MHK", "MHT", "MIA", "MKE", "MKG", "MLB", "MLI", "MLU", "MMH",
    "MOB", "MOT", "MQT", "MRY", "MSN", "MSO", "MSP", "MSY", "MTJ", "MVY",
    "MYR", "OAJ", "OAK", "OGG", "OKC", "OMA", "OME", "ONT", "ORD", "ORF",
    "ORH", "OTH", "OTZ", "PAH", "PBG", "PBI", "PDX", "PHF", "PHL", "PHX",
    "PIA", "PIB", "PIH", "PIT", "PLN", "PNS", "PPG", "PSC", "PSE", "PSG",
    "PSP", "PUB", "PVD", "PWM", "RAP", "RDD", "RDM", "RDU", "RHI", "RIC",
    "RKS", "RNO", "ROA", "ROC", "ROW", "RST", "RSW", "SAF", "SAN", "SAT",
    "SAV", "SBA", "SBN", "SBP", "SCC", "SCE", "SDF", "SEA", "SFO", "SGF",
    "SGU", "SHV", "SIT", "SJC", "SJT", "SJU", "SLC", "SMF", "SMX", "SNA",
    "SPI", "SPS", "SRQ", "STC", "STL", "STT", "STX", "SUN", "SUX", "SWF",
    "SYR", "TLH", "TOL", "TPA", "TRI", "TTN", "TUL", "TUS", "TVC", "TWF",
    "TXK", "TYR", "TYS", "UST", "VEL", "VLD", "VPS", "WRG", "WYS", "XNA",
    "YAK", "YUM"
]

loyalty_statuses = [
    "Standard",  
    "Bronze",     
    "Silver",    
    "Gold",      
    "Platinum"   
]

aircraftTypes = [
    # Narrow-body jets
    "Boeing 737",
    "Airbus A320",
    "Boeing 757",
    "Airbus A321",
    "Boeing 717",
    
    # Wide-body jets
    "Boeing 777",
    "Boeing 787",
    "Airbus A330",
    "Boeing 767",
    "Airbus A350",
    
    # Regional jets
    "Embraer E175",
    "Embraer E190",
    "Bombardier CRJ900",
    "Bombardier CRJ700",
    "Embraer ERJ145",
    
    # Propeller aircraft
    "ATR 72",
    "Bombardier Q400",
    "Saab 340"
];

paymentMethods = [
    "VISA",
    "PAYPAL",
    "BANK",
    "CASH"
]

currency_codes = [
    "USD",  # US Dollar
    "EUR",  # Euro
    "GBP",  # British Pound
    "JPY",  # Japanese Yen
    "CAD",  # Canadian Dollar
    "AUD",  # Australian Dollar
    "CHF",  # Swiss Franc
    "CNY",  # Chinese Yuan
    "HKD",  # Hong Kong Dollar
    "NZD",  # New Zealand Dollar
    "SEK",  # Swedish Krona
    "KRW",  # South Korean Won
    "SGD",  # Singapore Dollar
    "NOK",  # Norwegian Krone
    "MXN",  # Mexican Peso
    "INR",  # Indian Rupee
    "RUB",  # Russian Ruble
    "ZAR",  # South African Rand
    "TRY",  # Turkish Lira
    "BRL",  # Brazilian Real
    "DKK",  # Danish Krone
    "PLN",  # Polish Zloty
    "THB",  # Thai Baht
    "IDR",  # Indonesian Rupiah
    "HUF",  # Hungarian Forint
    "CZK",  # Czech Koruna
    "CLP",  # Chilean Peso
    "PHP",  # Philippine Peso
    "AED",  # UAE Dirham
    "COP",  # Colombian Peso
    "SAR",  # Saudi Riyal
    "MYR",  # Malaysian Ringgit
    "RON",  # Romanian Leu
    "ARS",  # Argentine Peso
    "EGP",  # Egyptian Pound
    "NGN",  # Nigerian Naira
    "PKR",  # Pakistani Rupee
    "BDT"   # Bangladeshi Taka
]

paymentStatus = [
    "PENDING",
    "CANCELLED",
    "ACCEPTED"
]

booking_classes = [
    "GUEST",
    "VIP",
    "MVP"
]

fare_types = [
    "Economy",
    "Premium Economy", 
    "Business",
    "First Class"
]

meteo_url = "https://archive-api.open-meteo.com/v1/archive"