# Input schema contract

The app accepts CSV data. Recommended columns:

| Column | Required | Description |
|---|---:|---|
| `facility_name` | Yes | Facility name. Aliases: `name`, `facility` |
| `region` | Yes | Region/province. Aliases: `state`, `region_name` |
| `district` | No | District name |
| `city` | No | City/town |
| `latitude` | No | Latitude for map. Alias: `lat` |
| `longitude` | No | Longitude for map. Aliases: `lon`, `lng` |
| `facility_type` | No | hospital/clinic/pharmacy/etc. |
| `operator_type` | No | public/private |
| `number_doctors` | No | Number of doctors. Alias: `doctors` |
| `capacity` | No | Bed capacity. Alias: `beds` |
| `notes` | Yes | Free-form facility text to parse |

If columns are missing, the pipeline fills safe defaults so the demo does not crash.
