package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLabelValueReplacement(t *testing.T) {
	assert := assert.New(t)

	assert.Equal("foo-xxx-u", LabelValueString("foo_"))
	assert.Equal("foo-xxx-u-u", LabelValueString("foo__"))
	assert.Equal("foo-xxx-u-h-u", LabelValueString("foo_-_"))
	assert.Equal("h-xxx-foo", LabelValueString("-foo"))
	assert.Equal("h-u-h-xxx-foo", LabelValueString("-_-foo"))
	assert.Equal("h-u-h-xxx-foo-bar-xxx-h-u-h", LabelValueString("-_-foo-bar-_-"))
	assert.Equal("u-u-u-xxx-foo_bar-xxx-u-u-u", LabelValueString("___foo_bar___"))
	assert.Equal("u-u-u-u-xxx-foo__bar-baz__quux-xxx-u-u-u-u", LabelValueString("____foo__bar--baz__quux____"))
}
