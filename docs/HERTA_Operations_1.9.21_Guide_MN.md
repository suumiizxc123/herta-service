# HERTA Operations ашиглах гарын авлага

Хувилбар **1.9.21** · Windows desktop · 2026 оны 10 сарын 23

Энэ гарын авлагаар HERTA-д нэвтрэх, REST болон Kafka connector тохируулах, ирсэн NIST файлыг шалгаж импортлох, subject экспортлох, автомат backup болон Telegram мэдэгдэл ажиллуулах дарааллыг тайлбарлана. Дэлгэц дээрх англи товчны нэрийг хэвээр нь хэрэглэсэн тул зааврыг апптай зэрэгцүүлэн дагахад тохиромжтой.

## 1 Анхны тохиргоо

### Апп болон worker хэрхэн ажиллах вэ

**Electron апп** нь тохиргоо хийх, өгөгдөл харах, импорт батлах удирдлагын интерфэйс. **Docker worker** нь REST polling, REST receiver, Kafka consumer, ирсэн файлын queue болон backup хуваарийг ажиллуулна. Аппын цонхыг хаахад worker үргэлжилнэ. Компьютер унтрах, sleep болох эсвэл Docker зогсоход ажиллахгүй.

### Эхлэхийн өмнө

- Windows дээр Docker Desktop суулгасан, engine нь ажиллаж байгаа байх.
- HERTA API URL, хэрэглэгчийн нэр, нууц үгээ бэлдэнэ.
- REST ашиглах бол endpoint болон шаардлагатай header; Kafka ашиглах бол broker, topic, consumer group, шаардлагатай TLS болон SASL мэдээллээ авна.
- Backup хадгалах бичих эрхтэй Windows хавтас бэлдэнэ. Жишээ нь `C:\HERTA-Backups`.

### Эхний удаа нээх дараалал

1. `HERTA Operations Setup 1.9.21.exe` суулгана. Portable ашиглах бол `HERTA Operations 1.9.21.exe` нээнэ.
2. HERTA server URL оруулна. Локал жишээ нь `https://localhost:8005`; өөр сервер бол тухайн серверийн хаягийг ашиглана.
3. Username, Password оруулаад **Sign in** дарна. Өөрийн итгэлтэй серверийн private эсвэл self-signed certificate ашиглаж байгаа үед **Verify server certificate** сонголтыг унтрааж болно.
4. **Dashboard → Docker background workers → Start workers** дарна. Ажиллаж байгаа stack шинэчлэх бол **Apply / start** дарна.
5. **Worker online** болон HERTA session холбогдсон эсэхийг шалгана. NIST backup-д worker HERTA серверт хандах боломжтой байх шаардлагатай.
6. **Connectors** руу орж **REST connector** эсвэл **Kafka connector** сонгоно. Холболтын алхмууд дараагийн хуудсуудад бий.

**Шинэ EXE суулгасны дараа:** Dashboard дээр **Apply / start** нэг удаа дарж, багцад орсон worker кодоор Docker image-ийг шинэчилнэ. EXE нь Docker Desktop-ийг өөрөө суулгахгүй.

<!-- PAGE -->
## 2 Connectors хэсэг

Connectors дээр **хоёр төрлийн connector** байна. Төрөл бүрд хэд хэдэн нэртэй холболт хадгалж болно. **Files** нь локал файл сонгох арга; PostgreSQL backup тохиргоо нь **Backup & restore** хэсэгт байна.

| Төрөл | Хэрэглээ | Үндсэн тохиргоо |
| --- | --- | --- |
| REST connector | API-аас татах эсвэл POST хүлээн авах | URL, headers, API pull эсвэл receiver горим |
| Kafka connector | Topic дээр шинээр ирсэн message хүлээн авах | Brokers, topic, consumer group, message format, TLS болон SASL |

### Холболт нэмэх ба өөрчлөх

1. **Connectors → Add REST** эсвэл **Add Kafka** дарна. Сонгосон төрлийн form шууд нээгдэнэ.
2. Тохиргоогоо оруулаад **Save connector** дарна. Save нь холболтыг хадгална; worker эхлүүлэхийн тулд **Activate** дарна.
3. **Test** дарж endpoint эсвэл broker хүрэх боломжтойг шалгана. Test амжилттай болох нь HERTA-д өгөгдөл импортлогдсон гэсэн үг биш.
4. Тохиргоо засахын өмнө **Deactivate** дарна. Дараа нь **Edit → Save connector → Activate** дарааллаар шинэ тохиргоогоо ажиллуулна.
5. **Remove** нь хадгалсан холболтыг устгана. Ирсэн queue-ийн файлуудыг Received inbox дээр тусад нь хянана.

### Төлөвийг унших

| Төлөв | Утга | Дараагийн үйлдэл |
| --- | --- | --- |
| Active | Worker дээр холболт идэвхтэй | Received болон Last activity үзүүлэлтийг шалгах |
| Inactive | Холболт хадгалагдсан боловч ажиллахгүй | Activate дарах |
| Worker offline | Docker worker-д хүрэхгүй байна | Docker Desktop, Dashboard дээрх stack-ийг шалгах |
| Not applied | Desktop тохиргоо worker-ийн төлөвт олдоогүй | Worker-оо шинэчлээд Activate тохиргоогоо дахин хэрэгжүүлэх |
| Error | API, broker, нэвтрэлт эсвэл message-ийн алдаа | Алдааны тайлбар, Test, Connector logs-ийг үзэх |

**Received** нь worker хүлээж авсан файлын тоо. **Last activity** нь хамгийн сүүлийн ажиллагааны цаг. REST pull дээр **Next poll** нь дараагийн API шалгалтын цаг. Эдгээр нь HERTA импортын амжилтын тоолуур биш.

### Хамт ажиллах холболтууд

REST pull хэд хэдэн endpoint-ийг зэрэг шалгаж болно. Одоогийн worker нэг Kafka consumer холболт, нэг REST receiver холболтыг идэвхтэй ажиллуулна. Өөр Kafka эсвэл receiver-ийг Activate хийхэд ижил төрлийн өмнөх идэвхтэй холболт унтарна. REST pull холболтууд тусдаа ажиллана.

<!-- PAGE -->
## 3 REST API аас өгөгдөл татах

Энэ горимд worker таны заасан API руу GET хүсэлт явуулж NIST файлын JSON хариу авна. API нь NIST файлыг Base64 хэлбэрээр буцаах ёстой. Ерөнхий subject JSON эсвэл зөвхөн зураг буцаадаг endpoint-ийг шууд NIST import болгохгүй.

### Тохиргооны жишээ

| Талбар | Оруулах жишээ |
| --- | --- |
| Connection name | `Иргэний бүртгэлийн API`; хоосон бол URL-аас нэр үүсгэнэ |
| How to receive data | `Pull from an API URL` |
| Endpoint URL | `https://api.example.com/nist` |
| Header name | `Authorization` |
| Header value | `Bearer YOUR_API_TOKEN` |

Жишээ хаяг болон token-ийг өөрийн бодит утгаар солино. **Headers → Add header** дарж шаардлагатай бусад header-ийг нэмнэ. Token-ийг URL дотор бичихийн оронд header ашиглана.

### Ажиллуулах дараалал

1. **Connectors → Add REST** нээгээд дээрх тохиргоогоо оруулна.
2. **Save connector → Test** дарна. Test нь өгөгдлийн envelope-ийг шалгах бөгөөд queue-д файл нэмэхгүй.
3. **Activate** дарна. Default тохиргоонд worker API-г 60 секунд тутам шалгана.
4. Шууд татах бол **Fetch now** дарна. Энэ нь хадгалсан URL, header-аа ашиглана.
5. **View REST received items** эсвэл **Import → REST API** нээж ирсэн файлаа сонгоно.

### API хариуны бүтэц

Нэг эсвэл олон файлыг дараах хэлбэрээр буцааж болно. `fileContent` дахь жишээ placeholder-ийг бодит NIST файлын Base64 утгаар солино.

```json
{
  "files": [
    {
      "fileName": "subject-001.nist",
      "fileContent": "BASE64_OF_REAL_NIST_FILE"
    }
  ]
}
```

`fileContent`-ийн оронд `dataBase64`, `files`-ийн оронд `items` эсвэл шууд массив мөн дэмжинэ. Файлын нэрийг `fileName`, `filename`, `code`, `name` талбаруудаас авна.

**No new files** гэвэл API ижил нэр, ижил byte агуулгатай файл буцаасан байж болно. Шинэ worker өмнө татсан өгөгдлийг давхар queue-д нэмэхгүй. Өмнөх файл импортлогдсон ч ижил өгөгдлийг шинэ файл мэт автоматаар дахин татахгүй.

<!-- PAGE -->
## 4 REST receiver ашиглан өгөгдөл хүлээн авах

Энэ горимд өөрийн програм эсвэл script worker-ийн endpoint руу NIST payload POST хийнэ. Polling endpoint шаардахгүй.

### Receiver тохируулах

1. **Connectors → Add REST** дарна.
2. **How to receive data → Receive POST requests** сонгоно. Хүсвэл Connection name оруулна.
3. **Save connector → Activate** дарна.
4. Холболтын **REST receiver URL and token** хэсгийг дэлгэнэ. URL болон Bearer token-оо авна.
5. POST амжилттай бол **Import → REST API** эсвэл **Received inbox** дээр файл харагдана. HERTA импортод оператор дахин сонгож батална.

### PowerShell ашиглан бодит NIST файл илгээх

Энэ жишээнд файл зам болон token-ийг өөрийн утгаар солино. `$body` нь жинхэнэ NIST файлын byte-уудаас Base64 үүсгэнэ.

```powershell
$file = 'C:\NIST\subject-001.nist'
$token = 'PASTE_RECEIVER_TOKEN_HERE'
$body = @{
  fileName = [IO.Path]::GetFileName($file)
  fileContent = [Convert]::ToBase64String(
    [IO.File]::ReadAllBytes($file)
  )
} | ConvertTo-Json

Invoke-RestMethod `
  -Uri 'http://127.0.0.1:18127/api/v1/nist' `
  -Method Post `
  -Headers @{ Authorization = "Bearer $token" } `
  -ContentType 'application/json' `
  -Body $body
```

Хариунд `accepted` болон queue-ийн `ids` ирвэл worker хүсэлтийг хүлээн авсан байна. Энэ нь HERTA subject үүссэн гэсэн баталгаа биш.

### Хүрэлцэх хүрээ ба token

Одоогийн Docker Compose port нь **127.0.0.1:18127** дээр bind хийгдсэн. Жишээ хүсэлтийг Docker host компьютер дээрээс ажиллуулна. Өөр компьютерээс POST хийхэд URL-ийн IP-г солих төдий хангалтгүй; тусдаа сүлжээний нийтлэлтийн тохиргоо шаардлагатай.

Receiver-ийг дахин асаах эсвэл worker restart хийхэд token солигдож болно. `401` алдаа гарвал идэвхтэй receiver-ийн одоогийн token-ийг дахин хуулна. HERTA login token болон receiver token нь өөр зориулалттай.

Нэг файл 32 MiB, нэг intake багц 256 MiB хүртэл; багцад 500 хүртэл файл дэмжинэ. Worker inbox нийт 500 item хүртэл хадгална. Base64 JSON нь эх файлын хэмжээнээс том болдгийг тооцно.

<!-- PAGE -->
## 5 Kafka connector тохируулах

Kafka connector нь broker дээрх topic-оос шинэ message-үүдийг Docker worker руу хүлээн авна. HERTA руу импортлохын өмнө Received inbox-д операторын хяналттайгаар хадгална.

### Broker сонгох

- **Бэлэн Kafka:** тухайн сүлжээний `broker1.example.com:9092` зэрэг хаяг, topic болон эрхийг админаасаа авна. Brokers талбарт `http://` бичихгүй.
- **Локал Kafka:** **Dashboard → Start a local Kafka broker → Apply / start** дарна. Desktop тохиргоонд broker-ийг `localhost:19092` гэж оруулна. Апп worker-ийн дотоод Kafka хаягийг өөрөө тохируулна.

| Талбар | Утга эсвэл жишээ |
| --- | --- |
| Connection name | `Үндсэн Kafka` |
| Bootstrap brokers | `broker1.example.com:9092,broker2.example.com:9092` |
| Topic | `herta.nist` |
| Consumer group | `herta-operations` эсвэл зориулалтын group |
| Message format | `JSON with Base64 NIST fileContent` эсвэл `Raw NIST bytes` |
| Use TLS | Broker TLS шаарддаг үед асаах |
| SASL | Broker-ийн тохиргоотой таарах mode, username, password |

### Холбож ажиллуулах

1. **Connectors → Add Kafka** нээгээд тохиргоогоо оруулж **Save connector** дарна.
2. **Test** дарж broker хүрэх, topic байгаа эсэхийг шалгана. Test амжилттай ч topic missing байж болно; тайлбарыг уншина.
3. Topic байхгүй бөгөөд үүсгэх эрхтэй бол **Create topic** дарна. Partitions, Replication factor нь broker-ийн боломжтой нийцэх ёстой. Нэг локал broker-д replication factor 1 хэрэглэнэ.
4. **Activate** дарна. Дараа нь producer-оос topic руу шинэ message илгээнэ.
5. **View Kafka received items** эсвэл **Import → Kafka** нээгээд ирсэн message-ийн файл, partition, offset, алдааг харна.

### Message format тааруулах

**JSON/Base64** сонгосон бол REST жишээтэй адил `fileName` болон `fileContent` бүхий JSON message илгээнэ. **Raw NIST bytes** сонгосон бол message value нь JSON биш, шууд NIST файлын byte-ууд байна. Төрөл зөрвөл message queue-д Error төлөвтэй орно.

Шинэ consumer topic-ийн эхнээс бүх хуучин message-ийг автоматаар уншихгүй. Анхны шалгалтаа **Activate хийсний дараах шинэ message**-ээр хийнэ. Topic үүсгэх нь message үүсгэхгүй.

Kafka offset нь холбогдох NIST inbox item импортлогдох эсвэл discard хийгдэх үед commit хийгдэнэ. **Remove selected** ашиглахдаа тэдгээр message-ийг боловсруулахгүйгээр алгасах үр дагаврыг тооцно. Worker restart болон delivery давталтын үед импортын үр дүнг HERTA талд давхар нягтална.

<!-- PAGE -->
## 6 Импорт экспорт болон subject харах

### Нэг дэлгэцээр импортлох

1. **Import** нээгээд **Files**, **REST API**, **Kafka** эх үүсвэрээ сонгоно.
2. Files үед **Browse for NIST files** дарна. REST/Kafka үед ready item-уудыг сонгож **Use selected items in import** дарна. Алдаатай item-ийг шууд импортлох боломжгүй.
3. **Selected import batch** дээр файл, хэмжээ, эх үүсвэрээ нягтална. Өөр batch сонгоход өмнөх сонгосон багцыг солино.
4. **HERTA group ID** оруулна. Default нь `0`; зориулагдсан group ID байгаа бол түүнийг хэрэглэнэ.
5. **Check files** дарна. Preview нь локал файл болон хэмжээг шалгаж, HERTA руу илгээхгүй. NIST стандартын тухайн хувилбарыг HERTA дэмжих эсэх нь бодит импортын үед баталгаажна.
6. **Import → IMPORT гэж бичих → Confirm import** дарна. Group болон server хаягийг баталгаажуулах цонхноос дахин шалгана.
7. Үр дүнг уншаад **Save report**-оор тайлангаа хадгална. **HTTP_SUCCESS** нь HTTP хүсэлт амжилттай болсныг заана; Subjects эсвэл HERTA client дээр subject үүссэнийг шалгана.

### Queue болон түүх

**Received inbox** дээр REST/Kafka-ийн бүх item-ийг харна. **All ready to import**, **All needing attention**, **Everything**, **Clear selection** цэсээр сонголтоо хийнэ. **Queue history** нь received, imported, discarded, failed үйл явдлыг; **Connector logs** нь асаалт, polling, backup болон connector алдааг харуулна.

Амжилттай импортлогдсон worker item inbox-оос хасагдана. Preview хийхэд хасагдахгүй. **Remove selected** нь queue-ээс item устгах үйлдэл бөгөөд импорт хийхгүй.

### Subject харах ба group шүүх

**Subjects** хэсэгт code эсвэл metadata-аар хайж, group filter-ээр шүүнэ. **View**, subject code эсвэл зураг дээр дарж дэлгэрэнгүйг нээнэ. Зураг харагдах эсэх нь HERTA record эсвэл NIST export дотор дэмжигдэх зураг байгаа эсэхээс хамаарна. Biometric profile-ийг шаардлагатай үед **Reveal profile**-оор харуулна.

Энэ хувилбарын subject дэлгэрэнгүй нь харах зориулалттай. Subject засах тусгай editor одоогоор байхгүй; өөрчлөлтийг HERTA-ийн үндсэн удирдлагаар хийнэ.

### NIST экспорт

1. **NIST export** эсвэл Subjects дээр record-уудаа сонгоно. **Select this page** болон **Select all matching** ашиглаж болно.
2. **Export selected** дарж ZIP хадгалах газраа сонгоно.
3. ZIP доторх NIST болон CSV тайланг шалгана. Нэг экспортод 500 хүртэл subject, нийт package 256 MiB хүртэл дэмжинэ. Олон subject бол group эсвэл хайлтаар багцлан экспортлоно.

<!-- PAGE -->
## 7 Автомат backup болон NIST сэргээх

### Backup хуваарь тохируулах

1. **Backup & restore → Choose folder** дарж Windows destination сонгоно. Жишээ нь `C:\HERTA-Backups`.
2. **Every minute**, **15 minutes**, **Hourly**, **Daily**, **Weekly** давтамжаас сонгоно. Every minute нь эхний ажиллагааг шалгахад тохиромжтой.
3. Daily/Weekly үед **Run at** талбараар цагаа сонгоно. Weekly нь Даваа гарагт ажиллана.
4. **Keep recent snapshots** дээр хадгалах тоогоо сонгоно.
5. **Turn on automatic backup**-ийг асааж **Save schedule** дарна. Давтамж сонгох төдийд worker-ийн хуваарь солигдохгүй.
6. **Worker next run**, destination, сүүлийн ажиллагааны төлөвийг шалгана. Анхны шалгалтаар **Run now** ажиллуулаад сонгосон Windows хавтсанд шинэ ZIP үүссэнийг нягтална.

Хуваарь нь desktop компьютерийн цагийн бүсээр worker-д хадгалагдана. **Asia/Ulaanbaatar** зэрэг харуулсан timezone-ийг шалгана. Worker файл хадгалахдаа `/backups/...` гэж лог бичиж болох бөгөөд энэ нь сонгосон Windows destination-ийн container доторх mount юм.

### Cron жишээнүүд

Advanced хэсэг нь таван талбартай cron хэрэглэнэ: минут, цаг, өдөр, сар, долоо хоногийн өдөр.

| Cron | Хэзээ ажиллах вэ |
| --- | --- |
| `* * * * *` | Минут тутам |
| `*/15 * * * *` | Цаг бүрийн 00, 15, 30, 45 минутад |
| `0 * * * *` | Цаг бүрийн эхэнд |
| `0 2 * * *` | Өдөр бүр 02:00 |
| `30 18 * * 1` | Даваа гараг бүр 18:30 |

Хуваарь ажиллахад компьютер асаалттай, sleep биш, Docker болон worker ажиллаж байх шаардлагатай. NIST snapshot-д HERTA access token хүчинтэй байх ёстой. **Sign out** хийхэд HERTA token worker-оос мөн арилна. Төлөвлөгөөт backup task нэг ажиллагааг нийт 3 хүртэл удаа оролдоно; гар ажиллагааны backup нэг удаа оролдоно.

### Snapshot сэргээх

**Restore snapshot → Choose ZIP**-ээр энэ аппын үүсгэсэн NIST snapshot ZIP сонгоно. Import хэсэгт файлуудыг preview хийж, group-ээ сонгоод IMPORT гэж батална. Энэ нь NIST subject дахин импортлох ажиллагаа; бүх database-ийг өмнөх цагт буцаах ажиллагаа биш. Давхардсан subject-ийг яаж боловсруулахыг HERTA шийднэ.

Хуучин ZIP-үүд retention тооноос хэтэрвэл амжилттай backup-ийн дараа цэвэрлэгдэнэ. HERTA subject олон бол нэг ZIP-ийн 500 subject болон 256 MiB хязгаарыг тооцож, бүрэн database backup хэрэгтэй үед PostgreSQL тохиргоог ашиглана.

<!-- PAGE -->
## 8 PostgreSQL backup болон өдөр тутмын ажиллагаа

PostgreSQL full database backup нь **Backup & restore → PostgreSQL full database backups** хэсэгт байна. Энэ нь REST/Kafka connector сонголтоос тусдаа backup тохиргоо.

### Database backup нэмэх

1. **Add database backup** дарна.
2. Connection name, host, port, database, username, password болон TLS mode оруулна. Docker host дээрх PostgreSQL-д хандах жишээ host нь `host.docker.internal`; database нэр болон эрхийг өөрийн орчноос авна.
3. **Save connector** дарж хадгалаад **Enable backup** дарна. Worker тухайн database-д холбогдож `pg_dump` ажиллуулах эрхтэй байх шаардлагатай.
4. Backup destination болон schedule-ээ тохируулаад **Save schedule** дарна. Идэвхтэй database backup-ууд автомат хуваарь болон worker-ийн гар backup task-д багтана.
5. Destination дээр `postgres-... .dump` файл үүссэн эсэх, алдааны мэдээллийг шалгана. PostgreSQL серверийн хувилбар worker-ийн `pg_dump` клиенттэй нийцэх шаардлагатай.

### ZIP ба DUMP файлын ялгаа

| Файл | Агуулга | Сэргээх арга |
| --- | --- | --- |
| NIST snapshot ZIP | HERTA API-аас экспортолсон subject NIST файлууд | Аппын Restore snapshot болон Import |
| PostgreSQL DUMP | Сонгосон database-ийн custom format logical dump | PostgreSQL-ийн `pg_restore` хэрэгсэлтэй тусдаа сэргээх ажиллагаа |

Database dump нь physical cluster image биш; бүх cluster-ийн roles, tablespaces-ийг бүрэн багтаахгүй. Аппын **Choose ZIP** товч `.dump` файлыг сэргээхгүй. Сэргээхээс өмнө тусдаа хоосон test database дээр DBA-тайгаа туршина.

Dump агуулгыг бичилт хийхгүйгээр жагсааж үзэх жишээ:

```powershell
pg_restore --list 'C:\HERTA-Backups\postgres-example.dump'
```

### Worker ажиллагааг тогтвортой байлгах

- Windows-д нэвтрэхэд Docker Desktop автоматаар эхлэх тохиргоог өөрийн орчинд тохируулна. Docker Desktop нь хэрэглэгч нэвтрэхээс өмнөх Windows service ажиллагаатай адил биш.
- Backup-ийн цагт компьютер sleep болохгүй байх нөхцөлийг хангана. Docker disk space болон destination-ийн сул зайг шалгана.
- Worker restart-ийн дараа хадгалсан connector-ууд, queue болон хуваарийн төлөвөө нягтална. Docker volume устгах ажиллагааг өдөр тутмын restart гэж үзэж болохгүй.
- Log дээр амжилттай гэсэн мөр байхаас гадна destination дээр файл үүссэн, хэмжээ нь бодит эсэхийг шалгана. Backup-аа үе үе тусдаа орчинд сэргээж туршина.

Docker stack-ийн тодорхойлолт нь `platform/compose.yaml`. Worker API/receiver нь локал 18127, нэмэлт Kafka нь 19092, нэмэлт Elasticsearch нь 19200 портуудаар нийтлэгдэнэ. Elasticsearch асаасан нь HERTA бүх өгөгдлийг индексэлсэн гэсэн үг биш.

<!-- PAGE -->
## 9 Telegram мэдэгдэл холбох

Worker нь REST эсвэл Kafka-аар NIST файл хүлээн авах үед Telegram мэдэгдэл илгээж болно. Одоогийн мэдэгдэл нь ирсэн файлын тухай; backup-ийн бүх үр дүн эсвэл бүх service-ийн мониторингийн alert гэж ойлгож болохгүй.

### Холбох дараалал

1. Telegram-ийн BotFather-оос өөрийн bot token авна. Token-оо бусдад илгээхгүй.
2. Өөрийн Telegram хэрэглэгчийн хаягаар тухайн bot-оо нээгээд **Start** эсвэл `/start` явуулна.
3. Аппын **Telegram → Bot token** талбарт token-оо оруулна. Docker worker online байх ёстой.
4. **Find chats** дарна. Олдсон хүн эсвэл группээ жагсаалтаас сонгоход destination Chat ID бөглөгдөнө. Өөрийн зөв Chat ID-г мэдэж байвал гараар оруулж болно.
5. **Save & connect** дарна. **Chat found** гэдэг нь bot болон chat шалгагдсаныг заана.
6. **Send test message** дарж мессеж яг зорьсон Telegram чатад ирснийг шалгана.

### Групп рүү илгээх

Bot-оо группт нэмээд тэнд `/start` зэрэг bot-д очих command явуулна. Дараа нь **Find chats**-ийг дахин дарж группээ сонгоно. Bot тухайн группт мессеж бичих эрхтэй байх ёстой. Chat ID-ийн сөрөг тэмдэг болон бүх цифрийг хэвээр хадгална.

### Chat олдохгүй үед

| Шинж | Шалгах зүйл |
| --- | --- |
| Find chats хоосон | Зөв bot token эсэх, /start-ыг яг тэр bot руу хувийн хэрэглэгчээр явуулсан эсэх; дахин /start явуулаад Find chats дарах |
| Webhook is active | Энэ bot өөр webhook ашиглаж байна. Одоо байгаа интеграцийг өөрчлөхгүйгээр зөв Chat ID-г гараар оруулах эсвэл эзэмшигчтэй нь тохиролцох |
| Another getUpdates request | Өөр апп ижил bot-ын update-ийг polling хийж байна. Давхцлыг админтайгаа зохицуулах |
| Bot cannot message another bot | Destination Chat ID нь bot-ын ID байна. Хүн эсвэл группийн Chat ID сонгох |
| Chat not found эсвэл blocked | Bot-оо нээгээд /start явуулах; block хийсэн бол unblock хийх; группийн эрх шалгах |

Bot token-ий эхний цифрүүд нь bot-ын өөрийн ID байж болно. Түүнийг destination chat ID болгож хуулж болохгүй. **Disconnect** нь Telegram тохиргоог салгана; REST/Kafka connector-ийг автоматаар зогсоохгүй.

<!-- PAGE -->
## 10 Алдаа оношлох болон меню

### Ажиллахгүй үед шалгах дараалал

1. **Dashboard:** Docker available, worker online, HERTA холболтыг шалгана.
2. **Connectors:** зөв төрөл, зөв saved connection, Active төлөв, Test үр дүнг шалгана.
3. **Import эсвэл Received inbox:** өгөгдөл ирсэн эсэх, ready/error item байгаа эсэхийг харна.
4. **Connector logs болон Queue history:** алдаа ямар шатанд гарсныг цагтай нь тулгана.
5. **HERTA эсвэл destination:** импортын subject болон backup файлын бодит үр дүнг шалгана.

| Асуудал | Хийх үйлдэл |
| --- | --- |
| Containers асахгүй | Docker Desktop engine-ээ ажиллуулаад Dashboard дээр Apply / start. Image build алдааг Logs-оос харах |
| REST 401 эсвэл 403 | Header name/value, Bearer token, endpoint-ийн эрхийг шалгах. Receiver бол одоогийн receiver token ашиглах |
| REST дээр fileContent байхгүй | API хариуг NIST Base64 envelope хэлбэрт оруулах. Ердийн subject JSON-ийг шууд импортлохгүй |
| Kafka шинэ message харахгүй | Active хийсний дараа шинэ message илгээх; topic, group, TLS/SASL, worker хүрэх advertised broker хаягийг шалгах |
| Kafka JSON parse алдаа | Producer JSON илгээж байна уу эсвэл raw bytes уу гэдгийг Message format-тай тааруулах |
| Import HTTP 400 | HERTA-ийн message-ийг нээж, group ID, NIST variant болон API эрхийг шалгах. Preview амжилттай нь сервер format хүлээн авна гэсэн баталгаа биш |
| Subject зураг харагдахгүй | View дээр дэлгэрэнгүйг нээж, HERTA record/NIST дотор зураг байгаа эсэхийг эх HERTA client-тай тулгах |
| Backup цаг өөрчлөгдөөгүй | Save schedule дарсан эсэх, worker-ийн timezone/next run, Docker image шинэчлэгдсэн эсэхийг шалгах |
| Backup destination хоосон | Worker last error, HERTA session, database эрх, Windows folder permission болон mount тохиргоог шалгах |

### Менюний зориулалт

**Dashboard** нь service-ийн төлөв болон worker удирдлага. **Connectors** нь REST/Kafka холболтууд. **Import** нь Files/REST/Kafka-аас багц бүрдүүлж HERTA руу оруулах хэсэг. **Received inbox**, **Queue history**, **Connector logs** нь queue болон түүний түүх.

**Subjects**, **NIST export**, **Profile check** нь HERTA өгөгдөл харах ба экспортлох үйлдлүүд. **Backup & restore** нь ZIP snapshot, schedule болон PostgreSQL dump тохиргоо. **Telegram** нь notification destination; **System health** нь HERTA API шалгалт; **Audit activity** нь ажиллагааны бүртгэл.

Энэ локал хувилбарт Debezium CDC, S3/MinIO archive, Prometheus/Grafana болон failover replication бүрэн хэрэгжээгүй. PostgreSQL dump болон NIST snapshot нь real-time database replica-ийг орлохгүй.
