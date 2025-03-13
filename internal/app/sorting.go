package parser

func QuickSort(aggregates []*Aggregate) {
	if len(aggregates) <= 1 {
		return
	}

	// Choose a pivot (middle element to avoid worst-case performance)
	pivotIndex := partition(aggregates)

	// Recursively sort the two partitions
	QuickSort(aggregates[:pivotIndex])
	QuickSort(aggregates[pivotIndex+1:])
}

func partition(aggregates []*Aggregate) int {
	// Use the middle element as the pivot
	pivotIndex := len(aggregates) / 2
	pivot := aggregates[pivotIndex].DocumentNumber

	// Move the pivot to the end
	aggregates[pivotIndex], aggregates[len(aggregates)-1] = aggregates[len(aggregates)-1], aggregates[pivotIndex]

	// Partition the slice
	storeIndex := 0
	for i := 0; i < len(aggregates)-1; i++ {
		if aggregates[i].DocumentNumber < pivot {
			aggregates[i], aggregates[storeIndex] = aggregates[storeIndex], aggregates[i]
			storeIndex++
		}
	}

	// Move the pivot to its final place
	aggregates[storeIndex], aggregates[len(aggregates)-1] = aggregates[len(aggregates)-1], aggregates[storeIndex]

	return storeIndex
}
