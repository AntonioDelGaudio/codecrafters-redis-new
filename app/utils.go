package main

import "strconv"

func addStringToInt(s string, i int) (string, error) {
	number, err := strconv.Atoi(s)
	if err != nil {
		return "", err
	}
	return strconv.Itoa(number + i), nil
}
