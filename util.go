package esclient

func RecordChunks(in chan Record, chunkSize int) chan []Record {
	output := make(chan []Record)

	go func() {
		chunk := make([]Record, chunkSize)
		i := 0

		for {
			rec, ok := <-in
			if !ok {
				break
			}
			chunk[i] = rec
			i = i + 1
			if i >= chunkSize {
				i = 0
				output <- chunk[0:chunkSize]
			}
		}

		if i > 0 {
			output <- chunk[0:i]
		}

		close(output)
	}()

	return output
}
