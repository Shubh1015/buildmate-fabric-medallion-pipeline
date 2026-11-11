# Stakeholder write-up: BuildMate Rentals, June 2026

**For:** Rhea (Head of Operations) and Arun (Finance Controller)
**Based on:** query (d) revenue by depot and query (f) machines currently out

Hadapsar Depot earns the most revenue, about ₹1.08 crore from 216 bills, out of ₹4.98 crore across all six depots. Per rental, though, Talegaon earns the most at ₹90,920 against Hadapsar's ₹49,934, so Hadapsar leads on volume while Talegaon leads on value per hire. Right now 175 machines are out on site, led by Chakan with 45 and Hadapsar with 40. This out-now figure only exists because the Silver quality filter is null-safe: a machine still out has a blank check-in, and the careless `filter(~bad)` would have silently deleted all 175 of them, so Rhea would have seen zero. Because bills mostly arrive after a machine returns, the machines still out are largely not yet billed, so depot revenue will rise as they come back.
