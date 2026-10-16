# Strengere Erkennung wiederkehrender Zahlungen, 03.10.2026

Die automatische Liste benötigt nun mindestens drei Zahlungen in drei Monaten,
einen gleichmäßigen Abstand von 1, 2, 3, 6 oder 12 Monaten, genau eine Zahlung
je belegtem Monat und einen Buchungstag innerhalb von ±4 Tagen des typischen
Kalendertags. Dieser Tag wird an kürzere Monate angepasst.

Die letzten bis zu sechs Beträge dürfen relativ zu ihrem Median höchstens
30 % auseinanderliegen. Unter 5 % wird die Zahlung als fix eingestuft. Eine
einzelne Erhöhung über 10 % bis maximal 50 % nach mindestens drei stabilen
Vorgängerzahlungen bleibt fix. Damit werden große Einzelkäufe nicht als
beliebig hohe Abo-Preiserhöhungen übernommen. Explizite Fix/Variabel-Einstufungen
und ausgeschlossene Einträge bleiben sichtbar bzw. wiederherstellbar;
ausgeschlossene Zahlungen bleiben aus den Prognosen entfernt.

## Prüfung und Deployment

- Lokal und auf dem Pi: 45 Tests erfolgreich, ein optionaler PDF-Test übersprungen.
- Migration und 13 Lese-Endpunkte auf einer Kopie geprüft; alle 450 Buchungen erhalten.
- Vergleich derselben Datenkopie am 03.10.2026: 38 → 16 sichtbare Einträge,
  30 → 15 aktive Einträge, 14 → 12 aktive feste und 16 → 3 aktive variable Einträge.
- Adresse: http://192.168.178.43:8001
- Image: `haushaltsbuch:strict-recurring-20261003`
- Vorheriges Image/Basis: `haushaltsbuch:recurring-exclusions-20261003`
- Quellen: `/home/shalm/haushaltsbuch-releases/codex-strict-recurring-20261003`
- Compose: `/home/shalm/haushaltsbuch-stack.yml`, Projekt `haushaltsbuch`
- Compose-Sicherung: `/home/shalm/haushaltsbuch-stack.before-strict-recurring-20261003.yml`
- SQLite-Sicherung im Datenvolume: `/app/data/backup-before-strict-recurring-20261003.db`

Ein Code-Rollback erfolgt durch Zurückkopieren der Compose-Sicherung und
`docker compose -p haushaltsbuch -f /home/shalm/haushaltsbuch-stack.yml up -d --no-deps haushaltsbuch`.
Die bestehende Datenbank bleibt dabei erhalten.
