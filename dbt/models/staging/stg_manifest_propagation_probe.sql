-- THROWAWAY PROBE MODEL — Phase 2 §9.7, gate condition 3.
--
-- ⚠️ DELETE THIS. It is not a model anyone should build on. It exists for exactly one
-- measurement: proving that a model added here appears as a NEW AIRFLOW TASK with no
-- edit to elt_dag.py and no Airflow image rebuild. That property — manifest drives the
-- DAG — is the entire reason the manifest-on-a-PVC route was chosen over git-sync.
--
-- Why a second model was needed at all: §9 was reprioritized ahead of §8, so there is
-- no real model to add yet. The property under test is manifest -> DAG propagation,
-- not the model, so `select 1` is sufficient.
--
-- 🚩 DELETION IS TRACKED. §8.4 must delete TWO placeholders in the same commit as the
-- first real staging model: stg_placeholder_events.sql and this file. A placeholder
-- that survives becomes a model someone believes in.
--
-- Deliberately carries NO tests, so the task-count change is unambiguous: with
-- test_behavior=AFTER_EACH a model without tests renders exactly one `.run` task, so
-- the DAG must move from 2 tasks to 3. A model with tests would add 2 and make the
-- arithmetic less pointed.

select 1 as probe_id

-- REUSED 2026-09-10 for §10.4's gate. This comment changes the manifest's checksum
-- and therefore forces a new CI run number onto it, WITHOUT adding a node. That is
-- deliberate: the §10.4 guard compares PROVENANCE (which CI run built each artifact),
-- not model lists, so the skew it must catch does not require a real new model. What a
-- new model would add is the model-not-found error the guard exists to pre-empt, and
-- §9.7 already measured that a new model becomes a new task with no DAG edit.
