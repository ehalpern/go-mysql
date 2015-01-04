package mysql

import (
	"gopkg.in/check.v1"
	"testing"
)

func Test(t *testing.T) {
	check.TestingT(t)
}

type mysqlTestSuite struct {
}

func (s *mysqlTestSuite) SetUpSuite(c *check.C) {

}

func (s *mysqlTestSuite) TearDownSuite(c *check.C) {

}

func (t *mysqlTestSuite) TestGTID(c *check.C) {
	us, err := ParseUUIDSet("de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2")
	c.Assert(err, check.IsNil)

	c.Assert(us.String(), check.Equals, "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2")

	buf := us.Encode()
	err = us.Decode(buf)
	c.Assert(err, check.IsNil)

	gs, err := ParseGTIDSet("de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2,de278ad0-2106-11e4-9f8e-6edd0ca20948:1-2")
	c.Assert(err, check.IsNil)

	buf = gs.Encode()
	err = gs.Decode(buf)
	c.Assert(err, check.IsNil)
}