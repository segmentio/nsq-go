package nsq

var (
	nsqlookups = []string{
		"localhost:4160", // nsqlookup-1
		"localhost:4162", // nsqlookup-2
		"localhost:4163", // nsqlookup-3
	}

	nsqd = []string{
		"localhost:4150", // nsqd-1
		"localhost:4152", // nsqd-2
		"localhost:4154", // nsqd-3
	}
)
