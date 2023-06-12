// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tablestore

import (
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/timer/api"
)

// CreateTimerTableSQL returns a SQL to create timer table
func CreateTimerTableSQL(dbName, tableName string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		ID BIGINT(64) UNSIGNED NOT NULL AUTO_INCREMENT,
		NAMESPACE VARCHAR(256) NOT NULL,
		TIMER_KEY VARCHAR(256) NOT NULL,
		TIMER_DATA BLOB,
		TIMEZONE VARCHAR(64) NOT NULL,
		SCHED_POLICY_TYPE VARCHAR(32) NOT NULL,
		SCHED_POLICY_EXPR VARCHAR(256) NOT NULL,
		HOOK_CLASS VARCHAR(64) NOT NULL,
		WATERMARK TIMESTAMP DEFAULT NULL,
		ENABLE TINYINT(2) NOT NULL,
		EVENT_STATUS VARCHAR(32) NOT NULL,
		EVENT_ID VARCHAR(64) NOT NULL,
		EVENT_DATA BLOB,
		EVENT_START TIMESTAMP DEFAULT NULL,
		SUMMARY_DATA BLOB,
		CREATE_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		UPDATE_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		VERSION BIGINT(64) UNSIGNED NOT NULL,
		PRIMARY KEY (ID),
		UNIQUE KEY timer_key(NAMESPACE, TIMER_KEY),
		KEY hook_class(HOOK_CLASS)
	)`, indentString(dbName, tableName))
}

func indentString(dbName, tableName string) string {
	return fmt.Sprintf("`%s`.`%s`", dbName, tableName)
}

func buildInsertTimerSQL(dbName, tableName string, record *api.TimerRecord) (string, []any) {
	var watermark, eventStart any
	watermarkFormat, eventStartFormat := "%?", "%?"
	if !record.Watermark.IsZero() {
		watermark = record.Watermark.Unix()
		watermarkFormat = "FROM_UNIXTIME(%?)"
	}

	if !record.EventStart.IsZero() {
		eventStart = record.EventStart.Unix()
		eventStartFormat = "FROM_UNIXTIME(%?)"
	}

	eventStatus := record.EventStatus
	if eventStatus == "" {
		eventStatus = api.SchedEventIdle
	}

	sql := fmt.Sprintf("INSERT INTO %s ("+
		"NAMESPACE, "+
		"TIMER_KEY, "+
		"TIMER_DATA, "+
		"TIMEZONE, "+
		"SCHED_POLICY_TYPE, "+
		"SCHED_POLICY_EXPR, "+
		"HOOK_CLASS, "+
		"WATERMARK, "+
		"ENABLE, "+
		"EVENT_ID, "+
		"EVENT_STATUS, "+
		"EVENT_START, "+
		"EVENT_DATA, "+
		"SUMMARY_DATA, "+
		"VERSION) "+
		"VALUES (%%?, %%?, %%?, 'TIDB', %%?, %%?, %%?, %s, %%?, %%?, %%?, %s, %%?, %%?, 1)",
		indentString(dbName, tableName),
		watermarkFormat,
		eventStartFormat,
	)

	return sql, []any{
		record.Namespace,
		record.Key,
		record.Data,
		string(record.SchedPolicyType),
		record.SchedPolicyExpr,
		record.HookClass,
		watermark,
		record.Enable,
		record.EventID,
		string(eventStatus),
		eventStart,
		record.EventData,
		record.SummaryData,
	}
}

func buildSelectTimerSQL(dbName, tableName string, cond api.Cond) (string, []any, error) {
	criteria, args, err := buildCondCriteria(cond, make([]any, 0, 8))
	if err != nil {
		return "", nil, err
	}

	sql := fmt.Sprintf("SELECT "+
		"ID, "+
		"NAMESPACE, "+
		"TIMER_KEY, "+
		"TIMER_DATA, "+
		"TIMEZONE, "+
		"SCHED_POLICY_TYPE, "+
		"SCHED_POLICY_EXPR, "+
		"HOOK_CLASS, "+
		"WATERMARK, "+
		"ENABLE, "+
		"EVENT_STATUS, "+
		"EVENT_ID, "+
		"EVENT_DATA, "+
		"EVENT_START, "+
		"SUMMARY_DATA, "+
		"CREATE_TIME, "+
		"UPDATE_TIME, "+
		"VERSION "+
		"FROM %s WHERE %s",
		indentString(dbName, tableName),
		criteria,
	)
	return sql, args, nil
}

func buildCondCriteria(cond api.Cond, args []any) (criteria string, _ []any, err error) {
	if cond == nil {
		return "1", args, nil
	}

	switch c := cond.(type) {
	case *api.TimerCond:
		criteria, args = buildTimerCondCriteria(c, args)
		return criteria, args, nil
	case *api.Operator:
		return buildOperatorCriteria(c, args)
	default:
		return "", nil, errors.Errorf("unsupported condition type: %T", cond)
	}
}

func buildTimerCondCriteria(cond *api.TimerCond, args []any) (string, []any) {
	items := make([]string, 0, cap(args)-len(args))
	if val, ok := cond.ID.Get(); ok {
		items = append(items, "ID = %?")
		args = append(args, val)
	}

	if val, ok := cond.Namespace.Get(); ok {
		items = append(items, "NAMESPACE = %?")
		args = append(args, val)
	}

	if val, ok := cond.Key.Get(); ok {
		if cond.KeyPrefix {
			items = append(items, "TIMER_KEY LIKE %?")
			args = append(args, val+"%")
		} else {
			items = append(items, "TIMER_KEY = %?")
			args = append(args, val)
		}
	}

	if len(items) == 0 {
		return "1", args
	}

	return strings.Join(items, " AND "), args
}

func buildOperatorCriteria(op *api.Operator, args []any) (string, []any, error) {
	if len(op.Children) == 0 {
		return "", nil, errors.New("children should not be empty")
	}

	var opStr string
	switch op.Op {
	case api.OperatorAnd:
		opStr = "AND"
	case api.OperatorOr:
		opStr = "OR"
	default:
		return "", nil, errors.Errorf("unsupported operator: %v", op.Op)
	}

	criteriaList := make([]string, 0, len(op.Children))
	for _, child := range op.Children {
		var criteria string
		var err error
		criteria, args, err = buildCondCriteria(child, args)
		if err != nil {
			return "", nil, err
		}

		if len(op.Children) > 1 && criteria != "1" && criteria != "0" {
			criteria = fmt.Sprintf("(%s)", criteria)
		}

		criteriaList = append(criteriaList, criteria)
	}

	criteria := strings.Join(criteriaList, " "+opStr+" ")
	if op.Not {
		switch criteria {
		case "0":
			criteria = "1"
		case "1":
			criteria = "0"
		default:
			criteria = fmt.Sprintf("!(%s)", criteria)
		}
	}
	return criteria, args, nil
}

func buildUpdateTimerSQL(dbName, tblName string, timerID string, update *api.TimerUpdate) (string, []any) {
	criteria, args := buildUpdateCriteria(update, make([]any, 0, 6))
	sql := fmt.Sprintf("UPDATE %s SET %s WHERE ID = %%?", indentString(dbName, tblName), criteria)
	return sql, append(args, timerID)
}

func buildUpdateCriteria(update *api.TimerUpdate, args []any) (string, []any) {
	updateFields := make([]string, 0, cap(args)-len(args))
	if val, ok := update.Enable.Get(); ok {
		updateFields = append(updateFields, "ENABLE = %?")
		args = append(args, val)
	}

	if val, ok := update.SchedPolicyType.Get(); ok {
		updateFields = append(updateFields, "SCHED_POLICY_TYPE = %?")
		args = append(args, string(val))
	}

	if val, ok := update.SchedPolicyExpr.Get(); ok {
		updateFields = append(updateFields, "SCHED_POLICY_EXPR = %?")
		args = append(args, val)
	}

	if val, ok := update.EventStatus.Get(); ok {
		updateFields = append(updateFields, "EVENT_STATUS = %?")
		args = append(args, string(val))
	}

	if val, ok := update.EventID.Get(); ok {
		updateFields = append(updateFields, "EVENT_ID = %?")
		args = append(args, val)
	}

	if val, ok := update.EventData.Get(); ok {
		updateFields = append(updateFields, "EVENT_DATA = %?")
		args = append(args, val)
	}

	if val, ok := update.EventStart.Get(); ok {
		if val.IsZero() {
			updateFields = append(updateFields, "EVENT_START = NULL")
		} else {
			updateFields = append(updateFields, "EVENT_START = FROM_UNIXTIME(%?)")
			args = append(args, val.Unix())
		}
	}

	if val, ok := update.Watermark.Get(); ok {
		if val.IsZero() {
			updateFields = append(updateFields, "WATERMARK = NULL")
		} else {
			updateFields = append(updateFields, "WATERMARK = FROM_UNIXTIME(%?)")
			args = append(args, val.Unix())
		}
	}

	if val, ok := update.SummaryData.Get(); ok {
		updateFields = append(updateFields, "SUMMARY_DATA = %?")
		args = append(args, val)
	}

	updateFields = append(updateFields, "VERSION = VERSION + 1")
	return strings.Join(updateFields, ", "), args
}

func buildDeleteTimerSQL(dbName, tblName string, timerID string) (string, []any) {
	return fmt.Sprintf("DELETE FROM %s WHERE ID = %%?", indentString(dbName, tblName)), []any{timerID}
}
