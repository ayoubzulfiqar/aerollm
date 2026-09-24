package notification

import (
	"errors"
	"os"
	"strings"
)

// SendersFromEnv builds the email and SMS backends configured through
// AEROLLM_SMTP_* and AEROLLM_TWILIO_* environment variables, for use as
// DispatcherOptions.EmailSender / SMSSender. A backend that is not
// configured is returned as a nil interface (so the dispatcher keeps
// reporting ErrChannelNotImplemented for it). A backend that is partially or
// invalidly configured is an error, so a typo does not silently disable
// alerting.
func SendersFromEnv() (EmailSender, SMSSender, error) {
	var (
		email EmailSender
		sms   SMSSender
		errs  []error
	)
	if o, ok := SMTPOptionsFromEnv(); ok {
		s, err := NewSMTPSender(o)
		if err != nil {
			errs = append(errs, err)
		} else {
			email = s
		}
	} else if anyEnvSet(EnvSMTPAddr, EnvSMTPFrom, EnvSMTPUser, EnvSMTPPassword) {
		errs = append(errs, errors.New("notification: incomplete SMTP configuration: "+EnvSMTPAddr+" and "+EnvSMTPFrom+" are required"))
	}
	if o, ok := TwilioOptionsFromEnv(); ok {
		t, err := NewTwilioSender(o)
		if err != nil {
			errs = append(errs, err)
		} else {
			sms = t
		}
	} else if anyEnvSet(EnvTwilioAccountSID, EnvTwilioAuthToken, EnvTwilioFrom) {
		errs = append(errs, errors.New("notification: incomplete Twilio configuration: "+EnvTwilioAccountSID+", "+EnvTwilioAuthToken+" and "+EnvTwilioFrom+" are required"))
	}
	return email, sms, errors.Join(errs...)
}

func anyEnvSet(names ...string) bool {
	for _, n := range names {
		if strings.TrimSpace(os.Getenv(n)) != "" {
			return true
		}
	}
	return false
}
