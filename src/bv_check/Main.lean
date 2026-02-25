import Std.Tactic.BVDecide

open Std

namespace SatCheck

/-- `String.trimAscii` now returns a `String.Slice`; convert it back to `String`. -/
private def trimStr (s : String) : String :=
  s.trimAscii.toString

/-- Split a line into whitespace-separated tokens (handles repeated spaces and tabs). -/
private def splitWords (s : String) : List String :=
  let s := trimStr (s.replace "\t" " ")
  (s.splitOn " ").filter (fun w => !w.isEmpty)

/-- Parse the DIMACS problem line: `p cnf <numVars> <numClauses>` -/
private def parseProblemLine (line : String) : IO (Nat × Nat) := do
  let ws := splitWords line
  match ws with
  | ["p", "cnf", nVars, nClauses] =>
      match nVars.toNat?, nClauses.toNat? with
      | some nv, some nc => pure (nv, nc)
      | _, _ =>
          throw <| IO.userError s!"DIMACS: bad problem line (expected naturals): {line}"
  | _ =>
      throw <| IO.userError s!"DIMACS: expected `p cnf <vars> <clauses>`, got: {line}"

/--
Parse a DIMACS CNF file into `Std.Sat.CNF Nat`.

DIMACS variables are 1-based; we map DIMACS var `k` to internal var `k-1`.
-/
def loadDimacsCNF (path : System.FilePath) : IO (Std.Sat.CNF Nat) := do
  let lines ← IO.FS.lines path

  -- Find exactly one problem line.
  let headerLines := lines.filter (fun l => (trimStr l).startsWith "p")
  if headerLines.size != 1 then
    throw <| IO.userError s!"DIMACS: expected exactly 1 problem line, found {headerLines.size}"

  let (numVars, numClauses) ← parseProblemLine headerLines[0]!

  -- Parse clause data. Clauses end at literal 0 and can span multiple lines.
  let mut clauses : List (Std.Sat.CNF.Clause Nat) := []
  let mut cur : Std.Sat.CNF.Clause Nat := []

  for raw in lines do
    let line := trimStr raw
    if line.isEmpty then
      continue
    if line.startsWith "c" || line.startsWith "p" then
      continue

    for tok in splitWords line do
      let some lit := tok.toInt?
        | throw <| IO.userError s!"DIMACS: non-integer token: {tok}"

      if lit == 0 then
        -- finish current clause
        clauses := cur.reverse :: clauses
        cur := []
      else
        -- DIMACS: ±k (k≥1). Map to Nat (k-1) and polarity Bool.
        let v : Nat := lit.natAbs - 1
        if decide (v < numVars) then
          let pol : Bool := decide (lit > 0)
          cur := (v, pol) :: cur
        else
          throw <| IO.userError s!"DIMACS: literal {lit} out of range (declared vars: {numVars})"

  -- Tolerate missing trailing 0 by finalizing any pending clause.
  if !cur.isEmpty then
    clauses := cur.reverse :: clauses

  let cnf := clauses.reverse

  if cnf.length == numClauses then
    pure cnf
  else
    throw <| IO.userError s!"DIMACS: header says {numClauses} clauses, but parsed {cnf.length}"

/-- Check `<cnfFile, lratFile>` using the same LRAT checker BVDecide uses internally. -/
def checkFiles (cnfFile lratFile : System.FilePath) : IO Bool := do
  let cnf ← loadDimacsCNF cnfFile
  let proof ← Std.Tactic.BVDecide.LRAT.loadLRATProof lratFile
  pure <| Std.Tactic.BVDecide.LRAT.check proof cnf

end SatCheck

/-- Lake executable entry point: must be named `main` in `Main.lean`. -/
def main (args : List String) : IO UInt32 := do
  match args with
  | [cnfFile, lratFile] =>
      let ok ← SatCheck.checkFiles (System.FilePath.mk cnfFile) (System.FilePath.mk lratFile)
      if ok then
        IO.println "VERIFIED"
        return 0
      else
        IO.eprintln "FAIL: LRAT certificate rejected."
        return 1
  | _ =>
      IO.eprintln "usage: bv_check <problem.cnf> <proof.lrat>"
      return 2
