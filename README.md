# Итоговый проект

### Описание
Репозиторий предназначен для сдачи итогового проекта.

### Как работать с репозиторием
1. В вашем GitHub-аккаунте автоматически создастся репозиторий `de-project-final` после того, как вы привяжете свой GitHub-аккаунт на Платформе.
2. Скопируйте репозиторий на свой компьютер. В качестве пароля укажите ваш `Access Token`, который нужно получить на странице [Personal Access Tokens](https://github.com/settings/tokens)):
	* `git clone https://github.com/Yandex-Practicum/de-project-final`
3. Перейдите в директорию с проектом: 
	* `cd de-project-final`
4. Выполните проект и сохраните получившийся код в локальном репозитории:
	* `git add .`
	* `git commit -m 'my best commit'`
5. Обновите репозиторий в вашем GitHub-аккаунте:
	* `git push origin main`

### Структура репозитория
Вложенные файлы в репозиторий будут использоваться для проверки и предоставления обратной связи по проекту. Поэтому постарайтесь публиковать ваше решение согласно установленной структуре — так будет проще соотнести задания с решениями.

Внутри `src` расположены папки:
- `/src/dags` - необходимо вложить код дага поставки данных из системы источника в наше хранилище данных. Назовите его `1_data_import.py`
Также необходимо разместить код дага обновления витрины данных `2_datamart_update.py`;
- `/src/sql` - здесь вы можете разместить sql запрос формирования таблиц в `STAGING` и `DWH` слоях, а также скрипт формирования данных для итоговой витрины;
- `/src/py` - при выборе реализации через kafka здесь разместите код запуска генерации данных в kafka topic и чтения;
- `/src/img` - здесь разместите скриншот реализованного вами дашборда над витриной данных.
