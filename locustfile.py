import random

from locust import HttpUser, between, task


class VegetableSalesUser(HttpUser):
    wait_time = between(0.01, 0.1)

    @task(50)
    def post_data(self):
        year = random.randint(2020, 2023)
        week = random.randint(1, 52)
        date = f"{year}-{week:02d}"

        vegetables = [
            "tomato",
            "carrot",
            "potato",
            "onion",
            "pepper",
            "tomate",
            "carotte",
            "pomme de terre",
            "oignon",
            "poivron",
        ]
        vegetable = random.choice(vegetables)

        kilo_sold = random.randint(50, 1000)

        self.client.post(
            "/post_sales/",
            json=[{"date": date, "vegetable": vegetable, "kilo_sold": kilo_sold}],
        )

    @task(25)
    def get_raw_sales(self):
        self.client.get("/get_raw_sales/")

    @task(25)
    def get_monthly_sales(self):
        remove_outliers = random.choice([True, False])
        self.client.get(
            f"/get_monthly_sales/?remove_outliers={str(remove_outliers).lower()}"
        )
