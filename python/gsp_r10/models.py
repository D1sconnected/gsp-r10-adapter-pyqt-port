from __future__ import annotations

from dataclasses import dataclass
import math


METERS_PER_S_TO_MILES_PER_HOUR = 2.2369
FEET_TO_METERS = 1 / 3.281


@dataclass(slots=True)
class BallData:
    speed: float
    spin_axis: float
    total_spin: float
    back_spin: float
    side_spin: float
    hla: float
    vla: float


@dataclass(slots=True)
class ClubData:
    speed: float
    speed_at_impact: float
    angle_of_attack: float
    face_to_target: float
    path: float


def ball_data_from_proto(ball_metrics: object | None) -> BallData | None:
    if ball_metrics is None:
        return None
    return BallData(
        speed=ball_metrics.ball_speed * METERS_PER_S_TO_MILES_PER_HOUR,
        spin_axis=ball_metrics.spin_axis * -1,
        total_spin=ball_metrics.total_spin,
        back_spin=ball_metrics.total_spin * math.cos(-1 * ball_metrics.spin_axis * math.pi / 180),
        side_spin=ball_metrics.total_spin * math.sin(-1 * ball_metrics.spin_axis * math.pi / 180),
        hla=ball_metrics.launch_direction,
        vla=ball_metrics.launch_angle,
    )


def club_data_from_proto(club_metrics: object | None) -> ClubData | None:
    if club_metrics is None:
        return None
    speed = club_metrics.club_head_speed * METERS_PER_S_TO_MILES_PER_HOUR
    return ClubData(
        speed=speed,
        speed_at_impact=speed,
        angle_of_attack=club_metrics.attack_angle,
        face_to_target=club_metrics.club_angle_face,
        path=club_metrics.club_angle_path,
    )
