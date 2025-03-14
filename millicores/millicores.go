package millicores

import (
	"context"

	"github.com/cockroachdb/apd"
	"github.com/cyverse-de/app-exposer/common"
	"github.com/cyverse-de/app-exposer/db"
	"github.com/cyverse-de/model/v8"
	"github.com/sirupsen/logrus"
)

var log = common.Log

type Detector struct {
	defaultNumber *apd.Decimal
	db            *db.Database
}

func New(db *db.Database, defaultNumber float64) (*Detector, error) {
	var defNum *apd.Decimal
	var err error

	defNum, err = apd.New(0, 0).SetFloat64(defaultNumber)
	if err != nil {
		return nil, err
	}
	return &Detector{
		defaultNumber: defNum,
		db:            db,
	}, nil
}

// NumberReserved scans the job to figure out the number of millicores reserved,
// and if it's not found there it uses the defaults.
func (d *Detector) NumberReserved(job *model.Job) (*apd.Decimal, error) {
	var err error

	reserved := apd.New(0, 0)
	log := log.WithFields(logrus.Fields{"context": "number reserved"})

	for _, step := range job.Steps {
		if step.Component.Container.MaxCPUCores != 0.0 {
			reserved, err = reserved.SetFloat64((float64(step.Component.Container.MaxCPUCores)))
			if err != nil {
				return nil, err
			}
			millisPerCPU := apd.New(1000, 0)
			_, err = apd.BaseContext.WithPrecision(15).Mul(reserved, reserved, millisPerCPU)
			if err != nil {
				return nil, err
			}
		} else {
			log.Debugf("reserved %s, default %s", reserved.String(), d.defaultNumber.String())
			_, err = apd.BaseContext.WithPrecision(15).Add(reserved, reserved, d.defaultNumber)
			if err != nil {
				return nil, err
			}
		}
	}
	return reserved, nil
}

func (d *Detector) StoreMillicoresReserved(context context.Context, job *model.Job, millicoresReserved *apd.Decimal) error {
	if job.ID != "" {
		log.Debugf("Storing millicores reserved by analysis ID: %s", job.ID)
		return d.db.SetMillicoresReservedByAnalysisID(context, job.ID, millicoresReserved)

	}
	log.Debugf("Storing millicores reserved by invocation ID: %s", job.InvocationID)
	return d.db.SetMillicoresReserved(context, job.InvocationID, millicoresReserved)
}
