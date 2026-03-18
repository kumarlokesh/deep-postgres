package wal

// XLOG resource manager (RmgrXlog).
//
// Handles checkpoint, WAL-switch, and backup records.
// Mirrors src/backend/access/transam/xlog.c — xlog_redo().
//
// For our redo engine the only interesting records are:
//   XLOG_CHECKPOINT_SHUTDOWN / ONLINE  — update the redo pointer
//   XLOG_SWITCH                        — segment switch; no page redo needed
// All other XLOG records are acknowledged but not acted upon.

import "fmt"

// XLOG sub-type constants (src/include/access/xlog_internal.h).
const (
	xlogCheckpointShutdown uint8 = 0x00
	xlogCheckpointOnline   uint8 = 0x10
	xlogNoop               uint8 = 0x20
	xlogNextOid            uint8 = 0x30
	xlogSwitch             uint8 = 0x40
	xlogBackupEnd          uint8 = 0x50
	xlogParameterChange    uint8 = 0x60
	xlogRestorePoint       uint8 = 0x70
	xlogFPW                uint8 = 0x80 // full_page_writes changed
	xlogEndOfRecovery      uint8 = 0x90
	xlogFPWChange          uint8 = 0xA0
	xlogOverwrite          uint8 = 0xB0
)

func xlogRedo(_ RedoContext) error {
	// All XLOG records are safe to skip for page-level redo.
	// Checkpoint handling (updating redo LSN, clearing hint-bit tracking)
	// would belong here in a full implementation.
	return nil
}

func xlogIdentify(rec *Record) string {
	op := rec.Header.XlInfo & xlHeapOpMask
	name := map[uint8]string{
		xlogCheckpointShutdown: "CHECKPOINT_SHUTDOWN",
		xlogCheckpointOnline:   "CHECKPOINT_ONLINE",
		xlogNoop:               "NOOP",
		xlogNextOid:            "NEXTOID",
		xlogSwitch:             "SWITCH",
		xlogBackupEnd:          "BACKUP_END",
		xlogParameterChange:    "PARAMETER_CHANGE",
		xlogRestorePoint:       "RESTORE_POINT",
		xlogFPW:                "FPW",
		xlogEndOfRecovery:      "END_OF_RECOVERY",
		xlogFPWChange:          "FPW_CHANGE",
		xlogOverwrite:          "OVERWRITE",
	}[op]
	if name == "" {
		name = fmt.Sprintf("op=0x%02X", op)
	}
	return fmt.Sprintf("XLOG/%s %s", name, rec.LSN)
}

func init() {
	Register(RmgrXlog, RmgrOps{
		Name:     "XLOG",
		Redo:     xlogRedo,
		Identify: xlogIdentify,
	})
}
