using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace LawernaTestDB
{
    internal class Program
    {
        static void Main(string[] args)
        {
            // Переменная с параметрами для подключения к БД, содержащая 
            // 1. название сервера, 2. имя пользователя, 3. название базы данных (модели), 4. пароль для подключения на сервер
            string connectionParameters = "server=localhost; user=root; database=lawerna_testdb; password=rafaelllo;";

            // Запуск функций, принимающих в качестве аргумента строку подключения к БД
            //firstTask(connectionParameters);
            //secondTask(connectionParameters);
            //thirdTask(connectionParameters);
            //fourthTask(connectionParameters);
            //fifthTask(connectionParameters);
            //sixthTask(connectionParameters);
            //seventhTask(connectionParameters);
            //eighthTask(connectionParameters);

            task("Задание 1.Вывести менеджеров, у которых имеется номер телефона", connectionParameters, "SELECT fio FROM managers WHERE phone <> 0;", 1);
            task("Задание 2. Вывести кол-во продаж за 20 июня 2021", connectionParameters, "SELECT count FROM sells WHERE date = '2021-06-20';", 1);
            task("Задание 3. Вывести среднюю сумму продажи с товаром 'ДСП 2440*1830*16 мм сорт 1'", connectionParameters, "SELECT AVG(summary) FROM sells WHERE id_product = 1;", 1);
            task("Задание 4. Вывести фамилии менеджеров и общую сумму продаж для каждого с товаром 'ОСБ'", connectionParameters, "SELECT managers.fio, sells.summary FROM sells JOIN managers ON sells.id_manager = managers.id WHERE id_product = 5;", 2);
            task("Задание 5. Вывести менеджера и товар, который продали 22 августа 2021", connectionParameters, "SELECT managers.fio, sells.date FROM sells JOIN managers ON sells.id_manager=managers.id WHERE date = '2021-08-22';", 2);
            task("Задание 6. Вывести все товары, у которых в названии имеется 'Фанера' и цена не ниже 1750", connectionParameters, "SELECT name FROM products WHERE name LIKE 'Фанера%' AND cost >= 1750;", 1);
            task("Задание 7. Вывести историю продаж товаров, группируя по месяцу продажи и наименованию товара", connectionParameters, "SELECT name, DATE_FORMAT(date, '%d-%m-%Y') FROM products, sells GROUP BY name, date ORDER BY date ASC;", 2);
            task("Задание 8. Вывести количество повторяющихся значений и сами значения из таблицы 'Товары', где количество повторений больше 1", connectionParameters, "SELECT cost, volume, COUNT(*) as duplicate FROM products GROUP BY cost, volume HAVING duplicate > 1;", 2);
        }

        static void task(string startMessage, string connectionParameters, string sqlRequest, int countOfOutputColumns)
        {
            Console.WriteLine("******\n" + startMessage);
            // Объект connection, принимающий строку подключения и дающий доступ к методу открытию соединения с БД
            MySqlConnection connection = new MySqlConnection(connectionParameters);
            connection.Open(); // открытие соединения с БД

            // Объект request обрабатывает SQL-запрос и посылает его на сервер
            MySqlCommand request = new MySqlCommand(sqlRequest, connection);

            // Объект, который читает ответ от сервера
            // Объект DataReader предоставляет небуферизованный поток данных,
            // позволяющий процедурам последовательно обрабатывать результаты из источника данных
            // Он хорошо подходит для извлечения больших объемов данных, поскольку данные не кэшируются в памяти
            MySqlDataReader reader = request.ExecuteReader();
            // Цикл осуществляет работу до тех пор, пока не выведет весь ответ с сервера в консоль
            while (reader.Read())
                switch (countOfOutputColumns)
                {
                    case 1:
                        Console.Write(reader[0].ToString() + "\n");
                        break;
                    case 2:
                        Console.Write(reader[0].ToString() + "\t" + reader[1].ToString() + "\n");
                        break;
                }

            reader.Close(); // прекращение чтения ответа от сервера
            connection.Close(); // закрытие соединения с БД
            Console.WriteLine("****** \n");
        }



        // Задание 1. Вывести менеджеров, у которых имеется номер телефона
        static void firstTask(string connectionParameters)
        {
            Console.WriteLine("******\nЗадание 1. Вывести менеджеров, у которых имеется номер телефона");
            // Объект connection, принимающий строку подключения и дающий доступ к методу открытию соединения с БД
            MySqlConnection connection = new MySqlConnection(connectionParameters);
            connection.Open(); // открытие соединения с БД

            // Данный SQL-запрос осуществляет выборку менеджеров, у которых поле phone не равно 0
            string sqlRequest = "SELECT fio FROM managers WHERE phone <> 0;";
            // Объект request обрабатывает SQL-запрос и посылает его на сервер
            MySqlCommand request = new MySqlCommand(sqlRequest, connection);

            // Объект, который читает ответ от сервера
            // Объект DataReader предоставляет небуферизованный поток данных,
            // позволяющий процедурам последовательно обрабатывать результаты из источника данных
            // Он хорошо подходит для извлечения больших объемов данных, поскольку данные не кэшируются в памяти
            MySqlDataReader reader = request.ExecuteReader();
            // Цикл осуществляет работу до тех пор, пока не выведет весь ответ с сервера в консоль
            Console.WriteLine("Менеджеры, у которых имеется номер телефона: ");
            while (reader.Read())
                Console.Write(reader[0].ToString() + "\n");

            reader.Close(); // прекращение чтения ответа от сервера
            connection.Close(); // закрытие соединения с БД
            Console.WriteLine("****** \n");
        }

        // Задание 2. Вывести количество продаж за 20 июня 2021
        static void secondTask(string connectionParameters)
        {
            string date = "2021-06-20";

            Console.WriteLine("******\nЗадание 2. Вывести кол-во продаж за 20 июня 2021");
            // Объект connection, принимающий строку подключения и дающий доступ к методу открытию соединения с БД
            MySqlConnection connection = new MySqlConnection(connectionParameters);
            connection.Open(); // открытие соединения с БД

            // Данный SQL-запрос осуществляет выборку количества продаж за 2021-06-20
            string sqlRequest = "SELECT count FROM sells WHERE date = '" + date + "';";
            // Объект request обрабатывает SQL-запрос и посылает его на сервер
            MySqlCommand request = new MySqlCommand(sqlRequest, connection);

            // Выполняет запрос и возвращает первый столбец первой строки в результирующий набор, возвращенный запросом
            string count = request.ExecuteScalar().ToString();
            Console.WriteLine("Количество продаж за " + date + " равно: " + count + " единиц");

            connection.Close(); // закрытие соединения с БД
            Console.WriteLine("****** \n");
        }

        // Задание 3. Вывести среднюю сумму продаж с товаром 'ДСП 2440*1830*16 мм сорт 1'
        static void thirdTask(string connectionParameters)
        {
            int productId = 1;

            Console.WriteLine("******\nЗадание 3. Вывести среднюю сумму продажи с товаром 'ДСП 2440*1830*16 мм сорт 1'");
            // Объект connection, принимающий строку подключения и дающий доступ к методу открытию соединения с БД
            MySqlConnection connection = new MySqlConnection(connectionParameters);
            connection.Open(); // открытие соединения с БД

            // Данный SQL-запрос осуществляет выборку менеджеров, у которых поле phone не равно 0
            string sqlRequest = "SELECT AVG(summary) FROM sells WHERE id_product = " + productId + ";";
            // Объект request обрабатывает SQL-запрос и посылает его на сервер
            MySqlCommand request = new MySqlCommand(sqlRequest, connection);

            // Выполняет запрос и возвращает первый столбец первой строки в результирующий набор, возвращенный запросом
            string summary = request.ExecuteScalar().ToString();
            Console.WriteLine("Средняя сумма продаж товара 'ДСП 2440*1830*16 мм сорт 1' равна: " + Math.Round(Convert.ToDecimal(summary)) + " рублей");

            connection.Close(); // закрытие соединения с БД
            Console.WriteLine("****** \n");
        }

        // Задание 4. Вывести фамилии менеджеров и общую сумму продаж для каждого с товаром 'ОСБ'
        static void fourthTask(string connectionParameters)
        {
            Console.WriteLine("******\nЗадание 4. Вывести фамилии менеджеров и общую сумму продаж для каждого с товаром 'ОСБ'");
            // Объект connection, принимающий строку подключения и дающий доступ к методу открытию соединения с БД
            MySqlConnection connection = new MySqlConnection(connectionParameters);
            connection.Open(); // открытие соединения с БД

            string sqlRequest = "SELECT managers.fio, sells.summary FROM sells JOIN managers ON sells.id_manager = managers.id WHERE id_product = 5;";
            // Объект request обрабатывает SQL-запрос и посылает его на сервер
            MySqlCommand request = new MySqlCommand(sqlRequest, connection);

            // Объект, который читает ответ от сервера
            // Объект DataReader предоставляет небуферизованный поток данных,
            // позволяющий процедурам последовательно обрабатывать результаты из источника данных
            // Он хорошо подходит для извлечения больших объемов данных, поскольку данные не кэшируются в памяти
            MySqlDataReader reader = request.ExecuteReader();
            // Цикл осуществляет работу до тех пор, пока не выведет весь ответ с сервера в консоль
            while (reader.Read())
                Console.Write(reader[0].ToString() + " - " + reader[1].ToString() + "\n");

            reader.Close(); // прекращение чтения ответа от сервера
            connection.Close(); // закрытие соединения с БД
            Console.WriteLine("****** \n");
        }

        // Задание 5. Вывести менеджера и товар, который продали 22 августа 2021
        static void fifthTask(string connectionParameters)
        {
            Console.WriteLine("******\nЗадание 5. Вывести менеджера и товар, который продали 22 августа 2021");
            // Объект connection, принимающий строку подключения и дающий доступ к методу открытию соединения с БД
            MySqlConnection connection = new MySqlConnection(connectionParameters);
            connection.Open(); // открытие соединения с БД

            string sqlRequest = "SELECT managers.fio, sells.date FROM sells JOIN managers ON sells.id_manager=managers.id WHERE date = '2021-08-22';";
            // Объект request обрабатывает SQL-запрос и посылает его на сервер
            MySqlCommand request = new MySqlCommand(sqlRequest, connection);

            // Объект, который читает ответ от сервера
            // Объект DataReader предоставляет небуферизованный поток данных,
            // позволяющий процедурам последовательно обрабатывать результаты из источника данных
            // Он хорошо подходит для извлечения больших объемов данных, поскольку данные не кэшируются в памяти
            MySqlDataReader reader = request.ExecuteReader();
            // Цикл осуществляет работу до тех пор, пока не выведет весь ответ с сервера в консоль
            while (reader.Read())
                Console.Write(reader[0].ToString() + " - " + reader[1].ToString() + "\n");

            reader.Close(); // прекращение чтения ответа от сервера
            connection.Close(); // закрытие соединения с БД
            Console.WriteLine("****** \n");
        }

        // Задание 6. Вывести все товары, у которых в названии имеется 'Фанера' и цена не ниже 1750
        static void sixthTask(string connectionParameters)
        {
            Console.WriteLine("******\nЗадание 6. Вывести все товары, у которых в названии имеется 'Фанера' и цена не ниже 1750");
            // Объект connection, принимающий строку подключения и дающий доступ к методу открытию соединения с БД
            MySqlConnection connection = new MySqlConnection(connectionParameters);
            connection.Open(); // открытие соединения с БД

            string sqlRequest = "SELECT name FROM products WHERE name LIKE 'Фанера%' AND cost >= 1750;";
            // Объект request обрабатывает SQL-запрос и посылает его на сервер
            MySqlCommand request = new MySqlCommand(sqlRequest, connection);

            // Объект, который читает ответ от сервера
            // Объект DataReader предоставляет небуферизованный поток данных,
            // позволяющий процедурам последовательно обрабатывать результаты из источника данных
            // Он хорошо подходит для извлечения больших объемов данных, поскольку данные не кэшируются в памяти
            MySqlDataReader reader = request.ExecuteReader();
            // Цикл осуществляет работу до тех пор, пока не выведет весь ответ с сервера в консоль
            while (reader.Read())
                Console.Write(reader[0].ToString() + "\n");

            reader.Close(); // прекращение чтения ответа от сервера
            connection.Close(); // закрытие соединения с БД
            Console.WriteLine("****** \n");
        }

        // Задание 7. Вывести историю продаж товаров, группируя по месяцу продажи и наименованию товара
        static void seventhTask(string connectionParameters)
        {
            Console.WriteLine("******\nЗадание 7. Вывести историю продаж товаров, группируя по месяцу продажи и наименованию товара");
            // Объект connection, принимающий строку подключения и дающий доступ к методу открытию соединения с БД
            MySqlConnection connection = new MySqlConnection(connectionParameters);
            connection.Open(); // открытие соединения с БД

            string sqlRequest = "SELECT name, DATE_FORMAT(date, '%d-%m-%Y') FROM products, sells GROUP BY name, date ORDER BY date ASC;";
            // Объект request обрабатывает SQL-запрос и посылает его на сервер
            MySqlCommand request = new MySqlCommand(sqlRequest, connection);

            // Объект, который читает ответ от сервера
            // Объект DataReader предоставляет небуферизованный поток данных,
            // позволяющий процедурам последовательно обрабатывать результаты из источника данных
            // Он хорошо подходит для извлечения больших объемов данных, поскольку данные не кэшируются в памяти
            MySqlDataReader reader = request.ExecuteReader();
            // Цикл осуществляет работу до тех пор, пока не выведет весь ответ с сервера в консоль
            while (reader.Read())
                Console.Write(reader[0].ToString() + "\t" + reader[1].ToString() + "\n");

            reader.Close(); // прекращение чтения ответа от сервера
            connection.Close(); // закрытие соединения с БД
            Console.WriteLine("****** \n");
        }

        // Задание 8. Вывести количество повторяющихся значений и сами значения из таблицы 'Товары', где количество повторений больше 1
        static void eighthTask(string connectionParameters)
        {
            Console.WriteLine("******\nЗадание 8. Вывести количество повторяющихся значений и сами значения из таблицы 'Товары', где количество повторений больше 1");
            // Объект connection, принимающий строку подключения и дающий доступ к методу открытию соединения с БД
            MySqlConnection connection = new MySqlConnection(connectionParameters);
            connection.Open(); // открытие соединения с БД

            string sqlRequest = "SELECT cost, volume, COUNT(*) as duplicate FROM products GROUP BY cost, volume HAVING duplicate > 1;";
            // Объект request обрабатывает SQL-запрос и посылает его на сервер
            MySqlCommand request = new MySqlCommand(sqlRequest, connection);

            // Объект, который читает ответ от сервера
            // Объект DataReader предоставляет небуферизованный поток данных,
            // позволяющий процедурам последовательно обрабатывать результаты из источника данных
            // Он хорошо подходит для извлечения больших объемов данных, поскольку данные не кэшируются в памяти
            MySqlDataReader reader = request.ExecuteReader();
            // Цикл осуществляет работу до тех пор, пока не выведет весь ответ с сервера в консоль
            while (reader.Read())
                Console.Write(reader[0].ToString() + "\t" + reader[1].ToString() + "\n");

            reader.Close(); // прекращение чтения ответа от сервера
            connection.Close(); // закрытие соединения с БД
            Console.WriteLine("****** \n");
        }
    }
}
