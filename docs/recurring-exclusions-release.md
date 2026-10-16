# Wiederkehrende Buchungen manuell ausschließen, 03.10.2026

„Nicht mehr wiederkehrend“ schließt einen Zahlungsstrom nach Quelle, Konto,
Händler und Kategorie dauerhaft aus anstehenden Terminen, aktiven Summen und
Fixkostenprognosen aus. Bestehende Buchungen und Fix/Variabel-Einstufungen bleiben
erhalten. Über „Ausgeschlossene anzeigen“ und „Wieder berücksichtigen“ ist der
Ausschluss rückgängig zu machen. Die additive Tabelle `recurring_exclusions`
speichert die Auswahl und berücksichtigt Kategorieumbenennungen.

## Deployment auf dem Pi

- Adresse: http://192.168.178.43:8001
- Compose: `/home/shalm/haushaltsbuch-stack.yml`, Projekt `haushaltsbuch`
- Quellen: `/home/shalm/haushaltsbuch-releases/codex-recurring-exclusions-20261003`
- Image: `haushaltsbuch:recurring-exclusions-20261003`
- Basis und vorheriges Image: `haushaltsbuch:splits-20260925`
- Compose-Sicherung: `/home/shalm/haushaltsbuch-stack.before-recurring-exclusions-20261003.yml`
- Konsistente SQLite-Sicherung im Datenvolume: `/app/data/backup-before-recurring-exclusions-20261003.db`

Die unveränderten Abhängigkeiten wurden vom bisherigen Image übernommen. Lokal
und auf dem Pi bestanden jeweils 41 Tests; ein optionaler Test mit privatem PDF
wurde übersprungen. Migration und 13 Lese-Endpunkte wurden auf einer Kopie der
450 produktiven Buchungen geprüft. Nach dem Containerwechsel wurden HTTP,
Datenbankintegrität, neue Tabelle, Erhalt der Buchungen und die Endpunkte für
wiederkehrende und anstehende Buchungen geprüft. Die Prüfsummen von Backend und
HTML stimmen mit dem lokalen Stand überein. Ausschließen und Wiederherstellen
wurden zuvor im Browser mit künstlichen Buchungen geprüft.

Für einen Code-Rollback die gesicherte Compose-Datei zurückkopieren und
`docker compose -p haushaltsbuch -f /home/shalm/haushaltsbuch-stack.yml up -d --no-deps haushaltsbuch`
ausführen. Das Datenvolume bleibt erhalten; das vorherige Image toleriert die
zusätzliche Tabelle. Die Datenbanksicherung nur gezielt wiederherstellen, da
sonst spätere Buchungen und Änderungen verloren gehen.
