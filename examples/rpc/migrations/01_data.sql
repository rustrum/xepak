-- Example data for the JSON-RPC todo application.
-- The rows intentionally use different titles, bodies, and statuses so search
-- and filtering can be tried immediately after startup.

INSERT INTO todos (title, body, status) VALUES
    ('Buy milk', 'Two liters of semi-skimmed milk, a loaf of sourdough bread, eggs, and coffee beans. Check the fridge first so we do not buy duplicates.', 'open'),
    ('Write quarterly report', 'Collect sales numbers from the CRM, compare them with the previous quarter, and prepare clear charts for the Q3 review. Add a short summary of the main risks and wins.', 'in_progress'),
    ('Fix checkout session bug', 'Users are logged out when the "session token" expires during checkout. Reproduce the issue with an expired token, refresh it before payment, and add a regression test for the checkout flow.', 'in_progress'),
    ('Plan team offsite', 'Find a venue for the engineering offsite in March. Compare prices and travel times, collect dietary requirements, and prepare a draft agenda with workshops and free time.', 'open'),
    ('Read Rust book', 'Finish the chapters about ownership, lifetimes, and async programming. Take notes on the examples, then rewrite one small module from the project using the new patterns.', 'open'),
    ('Renew passport', 'Book an appointment at the passport office before the summer trip. Prepare the old passport, two photos, proof of address, and the payment confirmation in advance.', 'done');

INSERT INTO todos (title, body, status) VALUES
    ('Book dentist appointment', 'Annual checkup and cleaning. Ask about the sensitive tooth on the left side and confirm whether the next appointment should be scheduled six or twelve months from now.', 'done'),
    ('Prepare presentation', 'Build the slides for the product roadmap meeting with the leadership team. Cover the current metrics, the next two milestones, open risks, and the decisions we need from the group.', 'in_progress'),
    ('Review pull request', 'Review the JSON-RPC pagination changes and leave feedback. Check the new input schemas, error handling, and tests, then verify the example queries against a fresh database.', 'open'),
    ('Update dependencies', 'Upgrade sqlx and actix-web to the latest compatible versions, fix any API changes, and run the full test suite. Check the changelogs for migration notes before merging.', 'open'),
    ('Clean garage', 'Sort the old tools, recycle the cardboard boxes, and sweep the floor. Put the camping gear on the top shelf and label the storage boxes so things are easier to find next time.', 'done'),
    ('Call insurance company', 'Ask about coverage for the new laptop and home office equipment. Confirm the deductible, whether accidental damage is included, and how to file a claim if something breaks.', 'open'),
    ('Design database schema', 'Draft tables for projects, tasks, and time tracking. Define primary keys, foreign keys, and indexes, then review the schema with the team before writing the first migration.', 'in_progress');

INSERT INTO todos (title, body, status) VALUES
    ('Write unit tests', 'Add tests for schema validation and Lua query builders. Cover required fields, invalid types, empty inputs, and pagination boundaries, then run the suite with coverage enabled.', 'open'),
    ('Order office chairs', 'Compare ergonomic chairs and order two for the home office. Check the return policy, lumbar support, and delivery date, and keep the packaging until both chairs are assembled.', 'done'),
    ('Plan vacation', 'Research train routes and book a hotel near the coast. Compare departure times, check cancellation policies, and reserve seats for the weekend before prices go up.', 'open'),
    ('Water the plants', 'The ficus and the herbs need water every other day. Check the soil first, remove dead leaves, and move the smaller pots closer to the window during the dark winter months.', 'done'),
    ('Backup laptop', 'Run the encrypted backup and verify the restore process. Check that the external drive has enough space, copy the latest photos, and test opening a few files from the backup.', 'open'),
    ('Learn SQLite FTS5', 'Practice phrase queries, ranking, and external content tables. Compare quoted phrases with plain terms, inspect the query plan, and write notes about tokenization and the unicode61 tokenizer.', 'in_progress'),
    ('Ship release', 'Tag version 0.0.8, publish the release notes, and announce the release. Verify the Docker image, update the documentation links, and monitor the issue tracker for the first day.', 'done');
