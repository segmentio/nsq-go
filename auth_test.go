package nsq

import "testing"

func TestAuth(t *testing.T) {
	tests := []struct {
		secret string
	}{
		{
			secret: "",
		},
		{
			secret: "0123456789",
		},
	}

	for _, test := range tests {
		t.Run("secret:"+test.secret, func(t *testing.T) {
			testCommand(t, "AUTH", Auth{
				Secret: test.secret,
			})
		})
	}
}
