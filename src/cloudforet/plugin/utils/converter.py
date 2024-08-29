class Converter(object):
    def __init__(self):
        super().__init__()

    @staticmethod
    def convert_priority_dict_to_priority_str(priority_dict: dict) -> str:
        avg_priority = Converter._calculate_avg_priority(priority_dict)
        return Converter._convert_avg_priority_to_priority(avg_priority)

    @staticmethod
    def _calculate_avg_priority(priority_dict: dict) -> float:
        sum_priority = 0
        total_count = 0
        for priority in priority_dict.keys():
            if priority == "P1":
                sum_priority += 1 * priority_dict[priority]
            elif priority == "P2":
                sum_priority += 2 * priority_dict[priority]
            elif priority == "P3":
                sum_priority += 3 * priority_dict[priority]
            else:
                sum_priority += 4 * priority_dict[priority]
            total_count += priority_dict[priority]
        return sum_priority / total_count

    @staticmethod
    def _convert_avg_priority_to_priority(avg_priority: float) -> str:
        if avg_priority < 1.5:
            return "P1"
        elif avg_priority < 2.5:
            return "P2"
        elif avg_priority < 3.5:
            return "P3"
        else:
            return "P4"
